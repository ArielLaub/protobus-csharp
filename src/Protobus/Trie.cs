using System.Collections.Generic;

namespace Protobus;

/// <summary>
/// Matches dotted topics against AMQP topic patterns, as the broker does: <c>*</c> is exactly
/// one word and <c>#</c> zero or more. Several values may share a pattern; <see cref="Match"/>
/// returns each matching value once. Not thread-safe: the event listener guards it.
/// </summary>
public sealed class Trie<T> where T : class
{
    private sealed class Node
    {
        internal readonly string Word;
        internal readonly bool Wildcard;
        internal readonly bool SuperWildcard;
        internal readonly List<T> Values = new();
        internal readonly Dictionary<string, Node> Children = new();
        internal readonly List<Node> Order = new();

        internal Node(string word)
        {
            Word = word;
            Wildcard = word is "*" or "#";
            SuperWildcard = word == "#";
        }

        internal Node Child(string word)
        {
            if (!Children.TryGetValue(word, out var n))
            {
                n = new Node(word);
                Children[word] = n;
                Order.Add(n);
            }
            return n;
        }
    }

    private readonly Node root = new("");

    public void Add(string pattern, T value)
    {
        var node = root;
        foreach (var word in pattern.Split('.')) node = node.Child(word);
        // Only the node the pattern ends on carries the value.
        node.Values.Add(value);
    }

    public List<T> Match(string topic)
    {
        var words = topic.Split('.');
        var ends = new List<Node>();
        foreach (var child in root.Order) Collect(child, words, 0, ends);
        var seen = new HashSet<T>(ReferenceEqualityComparer.Instance);
        var results = new List<T>();
        foreach (var node in ends)
            foreach (var v in node.Values)
                if (seen.Add(v)) results.Add(v);
        return results;
    }

    private static void Add(List<Node> ends, Node n)
    {
        if (!ends.Contains(n)) ends.Add(n);
    }

    private static void Collect(Node node, string[] words, int i, List<Node> ends)
    {
        if (node.SuperWildcard)
        {
            // '#' consumes zero words: hand words[i..] to the children; or one more, staying on '#'.
            if (i == words.Length && node.Values.Count > 0) Add(ends, node);
            foreach (var child in node.Order) Collect(child, words, i, ends);
            if (i < words.Length) Collect(node, words, i + 1, ends);
            return;
        }
        if (i >= words.Length) return;
        if (!node.Wildcard && node.Word != words[i]) return;
        var next = i + 1;
        if (next == words.Length)
        {
            // A pattern ends here if anything was registered on this node,
            // independently of whether longer patterns branch off it.
            if (node.Values.Count > 0) Add(ends, node);
            if (node.Children.TryGetValue("#", out var hash)) Collect(hash, words, next, ends);
            return;
        }
        foreach (var child in node.Order) Collect(child, words, next, ends);
    }
}
