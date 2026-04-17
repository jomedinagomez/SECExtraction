import json, sys

path = sys.argv[1] if len(sys.argv) > 1 else "output/trno_10Q_Q3_2024_targeted.json"
data = json.loads(open(path, encoding="utf-8").read())
tables = data["contents"][0]["fields"]["financialTables"]["valueArray"]
print(f"{len(tables)} tables extracted\n")

for i, t in enumerate(tables, 1):
    obj = t.get("valueObject", {})
    title = obj.get("tableTitle", {}).get("valueString", "?")
    stype = obj.get("statementType", {}).get("valueString", "?")
    rows = obj.get("rows", {}).get("valueArray", [])
    headers = [h.get("valueString", "") for h in obj.get("periodHeaders", {}).get("valueArray", [])]
    print(f"[{i}] {stype}: {title}")
    print(f"    Headers: {headers}  |  Rows: {len(rows)}")
    for r in rows:
        ro = r.get("valueObject", {})
        li = ro.get("lineItem", {}).get("valueString", "")
        vals = [v.get("valueString", "") for v in ro.get("values", {}).get("valueArray", [])]
        level = ro.get("level", {}).get("valueInteger", 0)
        is_hdr = ro.get("isSectionHeader", {}).get("valueBoolean", False)
        is_sub = ro.get("isSubtotal", {}).get("valueBoolean", False)
        parent = ro.get("parentLineItem", {}).get("valueString", "")
        flags = []
        if is_hdr:
            flags.append("HDR")
        if is_sub:
            flags.append("SUB")
        indent = "  " * level
        flag_str = " ".join(flags)
        print(f"    {indent}{li:<55s} {str(vals):<40s} {flag_str}  parent={parent}")
    print()
