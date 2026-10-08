using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Protobus.Tests;

/// <summary>
/// Compiles every C# snippet in the README and docs/ that is marked
/// <c>&lt;!-- doc-check: compile --&gt;</c>, against the runtime and the examples' generated code
/// (the Calculator, Chat and Combat schemas). The snippets of one page compile together, as a
/// reader assembles them: a later snippet may use a type an earlier one defined.
/// </summary>
public class DocSnippetTests
{
    private static readonly Regex Snippet = new("<!-- doc-check: compile -->\\s*```csharp\\n(.*?)```", RegexOptions.Singleline);

    private static string Root => typeof(DocSnippetTests).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
        .First(a => a.Key == "DocsRoot").Value!;

    public static TheoryData<string> Pages()
    {
        var pages = new TheoryData<string> { "README.md" };
        var docs = Path.Combine(Root, "docs");
        if (Directory.Exists(docs))
            foreach (var f in Directory.EnumerateFiles(docs, "*.md").OrderBy(f => f, StringComparer.Ordinal))
                pages.Add(Path.GetRelativePath(Root, f));
        return pages;
    }

    private static IEnumerable<MetadataReference> References()
    {
        var tpa = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        var local = Directory.EnumerateFiles(AppContext.BaseDirectory, "*.dll");
        return tpa.Concat(local).GroupBy(Path.GetFileName).Select(g => MetadataReference.CreateFromFile(g.First()));
    }

    [Fact]
    public void TheDocsHaveCheckedSnippets()
    {
        var total = Pages().Sum(p => Snippet.Matches(File.ReadAllText(Path.Combine(Root, (string)p.Data)))
            .Count);
        Assert.True(total > 0, "no doc-check snippets found");
    }

    [Theory]
    [MemberData(nameof(Pages))]
    public void EveryMarkedSnippetCompiles(string page)
    {
        var text = File.ReadAllText(Path.Combine(Root, page));
        var trees = Snippet.Matches(text).Select(m =>
        {
            var line = text.Substring(0, m.Index).Split('\n').Length;
            return CSharpSyntaxTree.ParseText(m.Groups[1].Value, new CSharpParseOptions(LanguageVersion.Latest), $"{page}:{line}");
        }).ToList();
        if (trees.Count == 0) return;
        var compilation = CSharpCompilation.Create("DocSnippets_" + Path.GetFileNameWithoutExtension(page), trees, References(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        var errors = compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, string.Join("\n", errors.Select(e => e.ToString())));
    }
}
