"""Flatten probe samples into pseudonymized CSVs for local Power BI exploration.

Reads samples/probe-*/record-*.json and writes samples/powerbi/*.csv.
User IDs, names, UPNs and IP addresses are never written; users appear only as
truncated SHA-256 hashes so joins still work. Output stays in the git-ignored
samples/ folder with owner-only permissions.
"""
import argparse
import csv
import glob
import hashlib
import json
import os
import re
import sys
from datetime import datetime

DURATION = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$")


def millis(value):
    match = DURATION.match(value or "")
    if not match:
        return None
    days, hours, minutes, secs = match.groups()
    total = int(days or 0) * 86400 + int(hours or 0) * 3600 + int(minutes or 0) * 60 + float(secs or 0)
    return round(total * 1000, 3)


def user_key(identity_id):
    if not identity_id:
        return None
    return "U-" + hashlib.sha256(identity_id.encode()).hexdigest()[:10]


def duration(start, end):
    if not start or not end:
        return None

    def parse(text):
        return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00")))

    return round((parse(end) - parse(start)).total_seconds(), 3)


def identity_id(identity):
    identity = identity or {}
    for node in (identity, identity.get("user") or {}, identity.get("guest") or {}):
        if node.get("id"):
            return node["id"]
    return None


def endpoint(ep):
    ep = ep or {}
    agent = ep.get("userAgent") or {}
    return {
        "user": user_key(identity_id(ep.get("associatedIdentity") or ep.get("identity"))),
        "platform": agent.get("platform"),
        "product": agent.get("productFamily"),
    }


def load_latest(root):
    latest = {}
    for path in glob.glob(os.path.join(root, "probe-*", "record-*.json")):
        with open(path, encoding="utf-8") as handle:
            snap = json.load(handle)
        record = snap["record"]
        key = (record["id"], record.get("version"))
        if key not in latest or snap["retrieved_at"] > latest[key]["retrieved_at"]:
            latest[key] = snap
    return [latest[key] for key in sorted(latest, key=str)]


def write(out_dir, name, rows):
    if not rows:
        return 0
    path = os.path.join(out_dir, name + ".csv")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def flatten(snapshots):
    calls, participants, sessions, segments, streams = [], [], [], [], []
    for snap in snapshots:
        r = snap["record"]
        cid = r["id"]
        organizer = user_key(identity_id((r.get("organizer_v2") or {}).get("identity") or r.get("organizer")))
        calls.append({
            "CallId": cid, "Version": r.get("version"), "CallType": r.get("type"),
            "Modalities": ",".join(r.get("modalities") or []),
            "StartDateTime": r.get("startDateTime"), "EndDateTime": r.get("endDateTime"),
            "DurationSeconds": duration(r.get("startDateTime"), r.get("endDateTime")),
            "OrganizerKey": organizer, "RetrievedAt": snap.get("retrieved_at"),
        })
        for p in r.get("participants_v2") or []:
            ident = p.get("identity") or {}
            pid = identity_id(ident)
            participants.append({
                "CallId": cid, "ParticipantKey": user_key(pid or p.get("id")),
                "IsGuest": bool(ident.get("guest")), "IsOrganizer": user_key(pid) == organizer,
            })
        for s in r.get("sessions") or []:
            sid = s["id"]
            caller, callee = endpoint(s.get("caller")), endpoint(s.get("callee"))
            fail = s.get("failureInfo") or {}
            sessions.append({
                "CallId": cid, "SessionId": sid, "Modalities": ",".join(s.get("modalities") or []),
                "StartDateTime": s.get("startDateTime"), "EndDateTime": s.get("endDateTime"),
                "DurationSeconds": duration(s.get("startDateTime"), s.get("endDateTime")),
                "CallerKey": caller["user"], "CallerPlatform": caller["platform"], "CallerProduct": caller["product"],
                "CalleePlatform": callee["platform"], "CalleeProduct": callee["product"],
                "FailureStage": fail.get("stage"), "FailureReason": fail.get("reason"),
            })
            for seg in s.get("segments") or []:
                gid = seg["id"]
                sfail = seg.get("failureInfo") or {}
                segments.append({
                    "CallId": cid, "SessionId": sid, "SegmentId": gid,
                    "StartDateTime": seg.get("startDateTime"), "EndDateTime": seg.get("endDateTime"),
                    "DurationSeconds": duration(seg.get("startDateTime"), seg.get("endDateTime")),
                    "FailureStage": sfail.get("stage"), "FailureReason": sfail.get("reason"),
                })
                for media in seg.get("media") or []:
                    cn, en = media.get("callerNetwork") or {}, media.get("calleeNetwork") or {}
                    for st in media.get("streams") or []:
                        streams.append({
                            "CallId": cid, "SessionId": sid, "SegmentId": gid,
                            "StreamId": st.get("streamId"), "MediaLabel": media.get("label"),
                            "Direction": st.get("streamDirection"),
                            "AudioCodec": st.get("audioCodec"), "VideoCodec": st.get("videoCodec"),
                            "AvgJitterMs": millis(st.get("averageJitter")), "MaxJitterMs": millis(st.get("maxJitter")),
                            "AvgRoundTripMs": millis(st.get("averageRoundTripTime")),
                            "MaxRoundTripMs": millis(st.get("maxRoundTripTime")),
                            "AvgPacketLossRate": st.get("averagePacketLossRate"),
                            "MaxPacketLossRate": st.get("maxPacketLossRate"),
                            "AvgConcealedRatio": st.get("averageRatioOfConcealedSamples"),
                            "MaxConcealedRatio": st.get("maxRatioOfConcealedSamples"),
                            "AvgAudioDegradation": st.get("averageAudioDegradation"),
                            "PacketUtilization": st.get("packetUtilization"),
                            "CallerConnectionType": cn.get("connectionType"), "CallerWifiBand": cn.get("wifiBand"),
                            "CalleeConnectionType": en.get("connectionType"), "CalleeWifiBand": en.get("wifiBand"),
                            "TransportProtocol": cn.get("networkTransportProtocol") or en.get("networkTransportProtocol"),
                        })
    return {"Calls": calls, "Participants": participants, "Sessions": sessions,
            "Segments": segments, "MediaStreams": streams}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--samples", default="samples")
    args = parser.parse_args()
    snapshots = load_latest(args.samples)
    if not snapshots:
        sys.exit("No samples found; run the probe first")
    out_dir = os.path.join(args.samples, "powerbi")
    os.makedirs(out_dir, mode=0o700, exist_ok=True)
    for name, rows in flatten(snapshots).items():
        print(f"{name}: {write(out_dir, name, rows)} rows")
    print(f"Wrote pseudonymized CSVs to {os.path.abspath(out_dir)}")


if __name__ == "__main__":
    main()
