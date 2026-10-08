namespace Protobus;

/// <summary>Validation of queue-level and per-message priorities, before anything reaches the broker.</summary>
public static class Priority
{
    /// <summary>
    /// Validate the queue-level <c>maxPriority</c> (<c>x-max-priority</c>). The floor is 1:
    /// <c>x-max-priority: 0</c> gives a single level, a plain queue with a priority queue's overhead.
    /// </summary>
    public static int? ValidateMaxPriority(int? value) => Require(value, "maxPriority", 1, 255,
        "RabbitMQ maintains internal structures per priority level, so keep the range small: "
        + $"{Config.RecommendedMaxPriority} is the recommended value and gives {Config.RecommendedMaxPriority + 1} levels.");

    /// <summary>Validate a per-message priority. 0 is valid and is RabbitMQ's default.</summary>
    public static int? ValidatePriority(int? value) => Require(value, "priority", 0, 255,
        "A priority above the queue's x-max-priority is clamped by the broker, not rejected.");

    private static int? Require(int? value, string label, int min, int max, string extra)
    {
        if (value == null) return null;
        if (value < min || value > max)
            throw new InvalidPriorityError($"{label} must be between {min} and {max}, got {value}. {extra}");
        return value;
    }
}
