using System;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Blanks C# comments and string and char literal text with spaces so textual architecture scans only see code.</summary>
/// <remarks>
/// The result has the same length as the source and keeps every line break, so match offsets and line numbers map back
/// to the source. Interpolation holes stay visible as code (literals nested in them are masked), format clauses are
/// masked, and preprocessor directive lines are left untouched. There is no semantic model: inactive
/// <c language="csharp">#if</c> branches are treated as ordinary code. Unterminated comments and literals are masked to
/// the end of the text.
/// </remarks>
internal static class CsharpSourceMasker
{
    /// <summary>Returns <paramref name="source"/> with comments and literal text replaced by spaces.</summary>
    /// <param name="source">The C# source text.</param>
    /// <returns>The masked text, with the same length and line breaks as the source.</returns>
    internal static string Mask(string source)
    {
        ArgumentNullException.ThrowIfNull(source);

        var text = source.ToCharArray();
        var position = 0;
        ScanCode(text, ref position, false);
        return new string(text);
    }

    private static void ScanCode(char[] text, ref int position, bool inHole)
    {
        var depth = 0;
        var atLineStart = !inHole;
        while (position < text.Length)
        {
            var current = text[position];
            if (current == '\n')
            {
                atLineStart = !inHole;
                position++;
                continue;
            }

            if (char.IsWhiteSpace(current))
            {
                position++;
                continue;
            }

            if (atLineStart && current == '#')
            {
                position = IndexOfLineEnd(text, position);
                continue;
            }

            atLineStart = false;
            if (TryMaskComment(text, ref position) || TryMaskLiteral(text, ref position))
                continue;

            if (inHole && IsHoleEnd(text, position, ref depth))
                return;

            position++;
        }
    }

    private static bool IsHoleEnd(char[] text, int position, ref int depth)
    {
        var current = text[position];
        if (current is '(' or '[' or '{')
        {
            depth++;
            return false;
        }

        if (current is ')' or ']' || (current == '}' && depth > 0))
        {
            depth--;
            return false;
        }

        // A top-level ':' starts the format clause; '::' is the alias qualifier.
        return current == '}'
            || (current == ':'
                && depth == 0
                && (position + 1 >= text.Length || text[position + 1] != ':')
                && (position == 0 || text[position - 1] != ':'));
    }

    private static bool TryMaskComment(char[] text, ref int position)
    {
        if (text[position] != '/' || position + 1 >= text.Length)
            return false;

        int end;
        if (text[position + 1] == '/')
        {
            end = IndexOfLineEnd(text, position);
        }
        else if (text[position + 1] == '*')
        {
            var close = text.AsSpan(position + 2).IndexOf("*/".AsSpan());
            end = close < 0 ? text.Length : position + 2 + close + 2;
        }
        else
        {
            return false;
        }

        MaskRange(text, position, end);
        position = end;
        return true;
    }

    private static bool TryMaskLiteral(char[] text, ref int position)
    {
        if (text[position] == '\'')
        {
            position = ScanCharLiteral(text, position);
            return true;
        }

        var quote = position;
        var dollars = 0;
        var verbatim = false;
        while (quote < text.Length)
        {
            if (text[quote] == '$')
                dollars++;
            else if (text[quote] == '@' && !verbatim)
                verbatim = true;
            else
                break;

            quote++;
        }

        if (quote >= text.Length || text[quote] != '"')
            return false;

        var quotes = CountRun(text, quote, '"');
        position = !verbatim && quotes >= 3
            ? ScanRawString(text, position, quote + quotes, quotes, dollars)
            : ScanQuotedString(text, position, quote + 1, verbatim, dollars > 0);
        return true;
    }

    private static int ScanCharLiteral(char[] text, int start)
    {
        var index = start + 1;
        while (index < text.Length)
        {
            var current = text[index];
            if (current == '\\')
            {
                index += 2;
                continue;
            }

            index++;
            if (current == '\'')
                break;
        }

        index = Math.Min(index, text.Length);
        MaskRange(text, start, index);
        return index;
    }

    private static int ScanQuotedString(char[] text, int start, int contentStart, bool verbatim, bool interpolated)
    {
        var literalStart = start;
        var index = contentStart;
        while (index < text.Length)
        {
            var current = text[index];
            if (current == '"' && !(verbatim && IsDoubled(text, index)))
            {
                index++;
                break;
            }

            if (interpolated && current == '{' && !IsDoubled(text, index))
            {
                index = ScanHole(text, ref literalStart, index + 1, 1);
                continue;
            }

            // Skips an escape sequence, a doubled verbatim quote, or a doubled interpolation brace as a pair.
            var pair = (current == '\\' && !verbatim) || current == '"' || (interpolated && current == '{');
            index += pair ? 2 : 1;
        }

        index = Math.Min(index, text.Length);
        MaskRange(text, literalStart, index);
        return index;
    }

    private static int ScanRawString(char[] text, int start, int contentStart, int quotes, int dollars)
    {
        var literalStart = start;
        var index = contentStart;
        while (index < text.Length)
        {
            var current = text[index];
            if (current == '"')
            {
                var run = CountRun(text, index, '"');
                index += run;
                if (run >= quotes)
                    break;

                continue;
            }

            if (dollars > 0 && current == '{')
            {
                var run = CountRun(text, index, '{');
                index = run >= dollars ? ScanHole(text, ref literalStart, index + run, dollars) : index + run;
                continue;
            }

            index++;
        }

        MaskRange(text, literalStart, index);
        return index;
    }

    private static int ScanHole(char[] text, ref int literalStart, int holeStart, int closingBraces)
    {
        MaskRange(text, literalStart, holeStart);
        var position = holeStart;
        ScanCode(text, ref position, true);

        // The format clause and the closing braces belong to the literal text.
        literalStart = position;
        var close = Array.IndexOf(text, '}', position);
        return close < 0 ? text.Length : Math.Min(close + closingBraces, text.Length);
    }

    private static bool IsDoubled(char[] text, int index) => index + 1 < text.Length && text[index + 1] == text[index];

    private static int CountRun(char[] text, int start, char value)
    {
        var index = start;
        while (index < text.Length && text[index] == value)
            index++;

        return index - start;
    }

    private static int IndexOfLineEnd(char[] text, int start)
    {
        var index = Array.IndexOf(text, '\n', start);
        return index < 0 ? text.Length : index;
    }

    private static void MaskRange(char[] text, int start, int end)
    {
        for (var index = start; index < end; index++)
        {
            if (text[index] is not ('\r' or '\n'))
                text[index] = ' ';
        }
    }
}
