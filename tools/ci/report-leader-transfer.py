"""Summarize election RPCs observed by the graceful-stop series on one runner."""

import json
import sys
from collections import Counter
from pathlib import Path
from statistics import median


def summarize(report, mode, replicas):
    samples = report["samples"]
    if not report["completed"] or len(samples) != 30:
        raise ValueError(f"{mode} RF={replicas}: incomplete stop series")
    rounds = 0
    transferred = 0
    phases = []
    for sample in samples:
        calls = sample["electionCalls"]
        hints = [call for call in calls if call["rpc"] == "hint"]
        recipients = {call["target"] for call in hints}
        expected = {"timer": 0, "successor": 1, "broadcast": replicas - 1, "lost": 1}[mode]
        if len(hints) != expected or len(recipients) != expected:
            raise ValueError(f"{mode} RF={replicas}, stop {sample['iteration']}: unexpected hint recipients {recipients}")
        if any(hint["sender"] != sample["stoppedLeader"] or hint["target"] == hint["sender"] for hint in hints):
            raise ValueError(f"{mode} RF={replicas}: invalid source or target")
        attempts = {(hint["target"], hint["term"] + 1) for hint in hints}
        if any((call["sender"], call["term"]) not in attempts for call in calls if call["rpc"] == "transfer-pre-vote"):
            raise ValueError(f"{mode} RF={replicas}: transfer pre-vote without matching hint")
        ballots = Counter((call["sender"], call["target"]) for call in calls
                          if call["rpc"] in ("pre-vote", "transfer-pre-vote"))
        # Every round contacts each other member. Count by the busiest target of each sender.
        rounds += sum(max(n for (node, _), n in ballots.items() if node == sender)
                      for sender in {node for node, _ in ballots})
        if mode in ("lost", "timer") and any(call["rpc"] == "transfer-pre-vote" for call in calls):
            raise ValueError(f"{mode} RF={replicas}: unexpected transfer pre-vote")
        winner = sorted((call for call in calls if call["sender"] == sample["newLeader"] and call["term"] == sample["newTerm"]),
                        key=lambda call: call["elapsedMs"])
        vote = next((call for call in winner if call["rpc"] == "vote"), None)
        probes = [call for call in winner if call["rpc"] in ("pre-vote", "transfer-pre-vote")]
        if not vote or not probes:
            raise ValueError(f"{mode} RF={replicas}: missing ballot for the winning term")
        # A later ordinary pre-vote in this same proposed term can recover from an unsuccessful hint.
        last_probe = max((call for call in probes if call["elapsedMs"] <= vote["elapsedMs"]), key=lambda call: call["elapsedMs"])
        transferred += last_probe["rpc"] == "transfer-pre-vote"
        first_probe = min(call["elapsedMs"] for call in calls if call["rpc"] in ("pre-vote", "transfer-pre-vote"))
        phases.append((first_probe, vote["elapsedMs"], sample["newLeaderMs"]))
    if (mode in ("successor", "broadcast")) != bool(transferred):
        raise ValueError(f"{mode} RF={replicas}: expected election path was never established")
    return rounds, transferred, phases


def main(root):
    print("| RF | Mode | Starts | Pre-vote rounds | Transfer wins | Timer wins | Client p50 ms | Client p95 ms |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    machines = set()
    milestones = []
    for rf, suffix in ((3, ""), (5, "-rf5")):
        for mode in ("timer", "successor", "broadcast", "lost"):
            path = root / f"mode-{mode}" / f"failover-leader-stop-series{suffix}.json"
            report = json.loads(path.read_text(encoding="utf-8-sig"))
            machines.add(report["machine"])
            rounds, transferred, phases = summarize(report, mode, rf)
            milestones.append((rf, mode, phases))
            count = len(report["samples"])
            print(f"| {rf} | {mode} | {count} | {rounds} | {transferred} | {count - transferred} | "
                  f"{report['fromStopStartP50Ms']:.0f} | {report['fromStopStartP95Ms']:.0f} |")
    print("\nMedian milliseconds from stop start to each observed milestone. Authority is sampled by the cluster observer.\n")
    print("| RF | Mode | First pre-vote | First vote by winner | Authority observed | Authority samples |")
    print("| --- | --- | --- | --- | --- | --- |")
    for rf, mode, phases in milestones:
        authority = [entry[2] for entry in phases if entry[2] is not None]
        observed = f"{median(authority):.0f}" if authority else "n/a"
        print(f"| {rf} | {mode} | {median(p[0] for p in phases):.0f} | {median(p[1] for p in phases):.0f} | {observed} | {len(authority)} |")
    if len(machines) != 1:
        raise ValueError("The series were recorded on different runners")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
