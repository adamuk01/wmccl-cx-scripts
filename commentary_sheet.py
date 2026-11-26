#!/usr/bin/env python3
"""Commentator rider list: entrants by race & league number, with data-derived notes.
Usage: commentary_sheet.py DATA_DIR ENTRY_CSV OUT_PREFIX [--history wmccl_history.db] [--prev-season 2025-26]
                           [--best N] [--title TXT]
Read-only on all DBs. Notes come purely from DB facts (no guessing)."""
import sqlite3, csv, re, sys, collections, argparse
from datetime import datetime
from xml.sax.saxutils import escape

DBS = ["U8","U10","U12","Youth","Women","Masters","Seniors"]
RACE_ORDER = ["Under 8","Under 10","Under 12","Youth (U-14 & U-16)","Junior Female","Junior Open",
              "Senior/Masters Female","Senior Open","Masters 40-49 Open","Masters 50+ Open"]
MERGE = {"SenF":"SenU23F","U23F":"SenU23F","SenM":"SenU23M","U23M":"SenU23M"}
LABEL = {"WomenAll":"Women's (all categories)","SenU23M":"Senior/U23 M","SenU23F":"Senior/U23 F"}
YOUNG = {"U8","U10","U12"}   # DBs where the Regionals round has real positions

def grp(db,cat):
    if db=="Women" and cat!="JunF": return "WomenAll"      # women ranked in one table (Women_All)
    return MERGE.get(cat,cat)
def glabel(g): return LABEL.get(g,g)
def nd(s):
    s=(s or "").strip()
    for f in ("%d-%b-%Y","%d-%b-%y","%m/%d/%y","%m/%d/%Y","%Y-%m-%d"):
        try:
            d=datetime.strptime(s.title() if f.startswith("%d-%b") else s,f)
            if d.year>2026: d=d.replace(year=d.year-100)
            return d.date().isoformat()
        except ValueError: pass
    return s
def nn(a,b): return re.sub(r"[^a-z]","",(a+b).lower())
def pts(n): return f"{n} pt{'' if n==1 else 's'}"
def ordinal(n): return f"{n}{'th' if 10<=n%100<=20 else {1:'st',2:'nd',3:'rd'}.get(n%10,'th')}"

def rank(lst,key="best"):
    lst.sort(key=lambda r:-r[key])
    for r in lst: r["pos"]=1+sum(1 for o in lst if o[key]>r[key]); r["of"]=len(lst)

def load(data_dir,best):
    riders={}
    for d in DBS:
        c=sqlite3.connect(f"file:{data_dir}/{d}.db?mode=ro",uri=True)
        res=collections.defaultdict(list)
        for rid,rnd,cp,op,p,ap,st in c.execute("select rider_id,round,cat_position,overall_position,points,is_ap,status from results"):
            res[rid].append(dict(rnd=rnd,cp=cp,op=op,pts=p,ap=ap,st=st))
        for (rid,num,fn,sn,club,cat,prev,dob) in c.execute(
            "select id,race_number,firstname,surname,club_name,race_category,race_category_previous_year,DOB from riders"):
            rs=res[rid]
            sc=sorted([r["pts"] for r in rs if r["pts"] is not None and r["pts"]!=999],reverse=True)
            real=[r for r in rs if r["st"]=="FIN" and not r["ap"] and r["pts"] not in (None,999)]
            riders[(nn(fn,sn),nd(dob))]=dict(db=d,num=num,name=f"{fn} {sn}",club=club or "",cat=cat,prev=prev,
                group=grp(d,cat),best=sum(sc[:best]),nscored=len(sc),real=real)
    groups=collections.defaultdict(list)
    for r in riders.values():
        if r["nscored"]>0: groups[r["group"]].append(r)
    for g,lst in groups.items():
        rank(lst)
        for r in lst:
            r["gap_lead"]=lst[0]["best"]-r["best"]; r["lead"]=lst[0]
            second=[o for o in lst if o["pos"]>1]
            r["gap_next"]=(r["best"]-second[0]["best"]) if (r["pos"]==1 and second) else None
    return riders

def load_history(path,season):
    """Previous-season final league (best 10 of effective points) and Regionals podium, by person."""
    c=sqlite3.connect(f"file:{path}?mode=ro",uri=True)
    people={p:(f"{fn} {sn}",nn(fn,sn),d) for p,fn,sn,d in c.execute("select person_id,firstname,surname,DOB from person")}
    ids={(v[1],nd(v[2])):p for p,v in people.items()}
    pts_by=collections.defaultdict(list)
    for sdb,num,rnd,eff in c.execute("select source_db,race_number,round,effective_points from round_results where season=? and effective_points is not null",(season,)):
        pts_by[(sdb,num)].append(eff)
    rows=[]
    for p,sdb,num,cat in c.execute("select person_id,source_db,race_number,race_category from season_stats where season=?",(season,)):
        sdb=sdb.replace(".db","")
        best=sum(sorted(pts_by.get((sdb+".db",num),pts_by.get((sdb,num),[])),reverse=True)[:10])
        rows.append(dict(person=p,name=people[p][0],db=sdb,cat=cat,group=grp(sdb,cat),best=best))
    groups=collections.defaultdict(list)
    for r in rows: groups[r["group"]].append(r)
    for g,l in groups.items(): rank(l)
    ly={r["person"]:r for r in rows}
    reg_round=None
    for rnd,note in c.execute("select round,note from season_rounds where season=?",(season,)):
        if note and note.lower().startswith("regional"): reg_round=rnd
    reg={}   # person -> (cat, pos) real positions at the Regionals (U8-U12 only)
    if reg_round:
        for sdb,num,cp,ap,st in c.execute("select source_db,race_number,cat_position,is_ap,status from round_results where season=? and round=?",(season,reg_round)):
            if cp and cp<=3 and not ap and st=="FIN":
                for r in rows:
                    pass
        ssmap={(r["db"],):0 for r in rows}
        idx={}
        for p,sdb,num,cat in c.execute("select person_id,source_db,race_number,race_category from season_stats where season=?",(season,)): idx[(sdb,num)]=(p,cat)
        for sdb,num,cp,ap,st in c.execute("select source_db,race_number,cat_position,is_ap,status from round_results where season=? and round=?",(season,reg_round)):
            if cp and cp<=3 and not ap and st=="FIN" and (sdb,num) in idx:
                p,cat=idx[(sdb,num)]; reg[p]=(cat,cp)
    return dict(ids=ids,ly=ly,groups=groups,reg=reg,people=people)

def notes_for(r,H=None,person=None):
    n=[]; g=glabel(r["group"]); pos=r.get("pos")
    if H and person is not None:
        l=H["ly"].get(person)
        l=l if (l and l["pos"]<=3) else None
        rg=H["reg"].get(person)
        W={1:"champion",2:"runner-up",3:"3rd"}; WR={1:"winner",2:"runner-up",3:"3rd"}
        extra=lambda l:(f" ({l['cat']})" if l["group"] in ("WomenAll","SenU23M","SenU23F") else "")
        if l and rg and rg[0]==l["cat"] and rg[1]==l["pos"]:
            n.append(f"{PREV} {glabel(l['group'])} league & Regional Champs {W[l['pos']]}"+extra(l))
        elif l and rg and rg[0]==l["cat"]:
            n.append(f"{PREV} {glabel(l['group'])} league {W[l['pos']]}, Regional Champs {WR[rg[1]]}"+extra(l))
        else:
            if l: n.append(f"{PREV} {glabel(l['group'])} league {W[l['pos']]}"+extra(l))
            if rg: n.append(f"{PREV} Regional Champs {rg[0]} {WR[rg[1]]}")
    wins=sorted(x["rnd"] for x in r["real"] if x["cp"]==1)
    pod=sum(1 for x in r["real"] if x["cp"] and x["cp"]<=3)
    last=max(r["real"],key=lambda x:x["rnd"]) if r["real"] else None
    if pos==1:
        gn=r.get("gap_next"); n.append(f"LEADS the {g} league"+(f" by {pts(gn)}" if gn else ""))
    elif pos in (2,3):
        n.append(f"{ordinal(pos)} in the {g} league, {pts(r['gap_lead'])} behind {r['lead']['name']}")
    if wins:
        n.append(f"{len(wins)} win{'s' if len(wins)>1 else ''} from {len(r['real'])} start{'s' if len(r['real'])!=1 else ''} (R{', R'.join(map(str,wins))})")
    elif pos and pos<=3 and pod:
        n.append(f"{pod} podium{'s' if pod>1 else ''} this season")
    if last and last["cp"]==1: n.append(f"won most recent round (R{last['rnd']})")
    if r["prev"] and r["prev"]!=r["cat"] and (pos and pos<=5 or wins): n.append(f"moved up from {r['prev']}")
    if not r["prev"] and (pos and pos<=5 or wins): n.append("new to the league (no previous-season record)")
    return n

def main():
    global PREV
    ap=argparse.ArgumentParser()
    ap.add_argument("data_dir"); ap.add_argument("entry_csv"); ap.add_argument("out")
    ap.add_argument("--history"); ap.add_argument("--prev-season",default="2025-26")
    ap.add_argument("--best",type=int,default=9); ap.add_argument("--title",default="WMCCL commentator rider list")
    a=ap.parse_args(); PREV=a.prev_season
    riders=load(a.data_dir,a.best)
    H=load_history(a.history,a.prev_season) if a.history else None
    rows=list(csv.DictReader(open(a.entry_csv,encoding="utf-8-sig")))
    races=collections.defaultdict(list); unmatched=[]
    for e in rows:
        key=(nn(e["First name"],e["Last name"]),nd(e["Date of birth"]))
        r=riders.get(key)
        if not r: unmatched.append(f"{e['First name']} {e['Last name']} ({e['Entry type']})"); continue
        r=dict(r); r["person"]=H["ids"].get(key) if H else None
        r["notes"]=notes_for(r,H,r["person"]); races[e["Entry type"]].append(r)
    order=[x for x in RACE_ORDER if x in races]+[x for x in races if x not in RACE_ORDER]
    with open(a.out+".csv","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["Race","Number","Name","Club","Category","League position","League points","Notes"])
        for rc in order:
            for r in sorted(races[rc],key=lambda r:r["num"]):
                w.writerow([rc,r["num"],r["name"],r["club"],r["cat"],f"{r.get('pos','-')}/{r.get('of','-')}",r["best"],"; ".join(r["notes"])])
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, PageBreak
    ss=getSampleStyleSheet()
    cell=ParagraphStyle("c",parent=ss["Normal"],fontSize=8.5,leading=10.5)
    bold=ParagraphStyle("b",parent=cell,fontName="Helvetica-Bold")
    small=ParagraphStyle("sm",parent=cell,fontSize=8,leading=9.8)
    hdr=ParagraphStyle("h",parent=ss["Heading1"],fontSize=16,spaceAfter=2)
    sub=ParagraphStyle("s",parent=ss["Normal"],fontSize=9,textColor=colors.grey,spaceAfter=6)
    def footer(c,d):
        c.saveState(); c.setFont("Helvetica",8); c.setFillColor(colors.grey)
        c.drawString(36,20,a.title); c.drawRightString(A4[0]-36,20,f"Page {d.page}"); c.restoreState()
    doc=SimpleDocTemplate(a.out+".pdf",pagesize=A4,leftMargin=36,rightMargin=36,topMargin=36,bottomMargin=36,title=a.title)
    story=[]
    for i,rc in enumerate(order):
        if i: story.append(PageBreak())
        lst=sorted(races[rc],key=lambda r:r["num"])
        story+=[Paragraph(escape(rc),hdr),
                Paragraph(f"{len(lst)} entrants · sorted by race number · league position is best {a.best} scores, shown as position/riders scoring in that category",sub)]
        if H:   # previous-season top 3 for the groups racing here
            entered={r["person"] for r in lst if r["person"] is not None}
            elsewhere={r2["person"]:rc2 for rc2 in order for r2 in races[rc2] if r2["person"] is not None}
            seen=[]; 
            for r in lst:
                if r["group"] not in seen: seen.append(r["group"])
            lines=[]
            for g in sorted(seen):
                # use the previous-season group of the same name (current categories map 1:1 except new riders)
                top=sorted([x for x in H["groups"].get(g,[]) if x["pos"]<=3],key=lambda x:(x["pos"],x["name"]))
                if not top: continue
                parts=[]
                for x in top:
                    tag="" if x["group"] not in ("WomenAll","SenU23M","SenU23F") else f" ({x['cat']})"
                    parts.append(f"{x['pos']}. {escape(x['name'])}{tag}"+("" if x["person"] in entered else (f" <i>(now racing {escape(elsewhere[x['person']])})</i>" if x["person"] in elsewhere else " <i>(not entered)</i>")))
                lines.append(f"<b>{escape(glabel(g))}:</b> "+" &nbsp; ".join(parts))
            if lines:
                story.append(Paragraph(f"<b>{PREV} final league top 3</b>",small))
                for L in lines: story.append(Paragraph(L,small))
                if rc in ("Under 8","Under 10","Under 12"):
                    pass
                story.append(Paragraph("&nbsp;",small))
        data=[[Paragraph(h,bold) for h in ["No.","Name","Club","Cat","League","Notes"]]]
        sty=[("GRID",(0,0),(-1,-1),0.25,colors.lightgrey),("BACKGROUND",(0,0),(-1,0),colors.HexColor("#dddddd")),
             ("VALIGN",(0,0),(-1,-1),"TOP"),("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2)]
        for j,r in enumerate(lst,1):
            lp=f"{r['pos']}/{r['of']}" if "pos" in r else "–"
            data.append([Paragraph(str(r["num"]),bold),Paragraph(escape(r["name"]),bold if r["notes"] else cell),
                         Paragraph(escape(r["club"]),cell),Paragraph(r["cat"],cell),Paragraph(lp,cell),
                         Paragraph(escape("; ".join(r["notes"])),cell)])
            if r["notes"]: sty.append(("BACKGROUND",(0,j),(-1,j),colors.HexColor("#fff4cc")))
        t=Table(data,colWidths=[30,100,105,38,48,A4[0]-72-321],repeatRows=1); t.setStyle(TableStyle(sty)); story.append(t)
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    print("races:",{rc:len(races[rc]) for rc in order}); print("unmatched (non-league, not listed):",len(unmatched))
    print("with notes:",sum(1 for rc in order for r in races[rc] if r["notes"]))
    if H: print("entrants linked to history:",sum(1 for rc in order for r in races[rc] if r["person"] is not None))
if __name__=="__main__": main()
