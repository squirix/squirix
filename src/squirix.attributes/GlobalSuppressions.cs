using System.Diagnostics.CodeAnalysis;

#pragma warning disable IDE0076
[assembly:
    SuppressMessage(
        "NDepend",
        "ND1407:AssembliesThatDontSatisfyTheAbstractnessInstabilityPrinciple",
        Justification = "Leaf attribute-only assembly; low abstractness with high instability is expected for a small dependency library.")]
#pragma warning restore IDE0076
