import tkinter as tk
from tkinter import ttk
import threading
import time
import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime
import queue

# ─── CONFIG ───────────────────────────────────────────────────────────────────
CSV_PATH = r"D:\ITI LABS\Grad project\datasets\raw_data\football_events_enriched2.csv"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "real_match_events"

SPEED_OPTIONS = {"x5": 5, "x10": 10, "x18": 18, "x30": 30}

# ─── EVENT TYPE DETECTION (من description لأنه مفيش type column) ──────────────
def detect_event_type(description):
    """استنتاج نوع الحدث من الـ description"""
    if pd.isna(description) or str(description).strip() == "":
        return "substitutions"
    desc = str(description).lower()
    if "yellow card" in desc or "second yellow" in desc or "red card" in desc:
        return "cards"
    elif ("goal" in desc or "scored" in desc or "missed" in desc or
          "shot" in desc or "header" in desc or "penalty" in desc or
          "free kick" in desc or "tap-in" in desc or "own-goal" in desc):
        return "goals"
    else:
        return "substitutions"

# ─── MATCH PHASE LOGIC ────────────────────────────────────────────────────────
def determine_match_structure(df):
    max_min = int(df["minute"].max())
    if max_min <= 90:
        end_minute = 90
        phase_label = "FULL TIME"
    elif max_min <= 120:
        end_minute = 120
        phase_label = "EXTRA TIME"
    else:
        end_minute = 120
        phase_label = "EXTRA TIME"
    return end_minute, phase_label

# ─── STADIUM NIGHT PALETTE ────────────────────────────────────────────────────
BG        = "#060b14"
CARD      = "#0e1623"
CARD2     = "#111d2e"
BORDER    = "#1c2e45"
ACCENT    = "#00d4ff"
ACCENT_DIM= "#005f72"
RED_TEAM  = "#ff6b35"
BLUE_TEAM = "#4fc3f7"
GREEN_LIVE= "#00e676"
YELLOW_EV = "#ffd600"
ORANGE_ET = "#ff9100"
TEXT      = "#dce8f5"
SUBTEXT   = "#5a7a9a"
WHITE     = "#ffffff"

F_MONO   = ("Courier New", 10)
F_MONO_S = ("Courier New", 9)
F_MONO_B = ("Courier New", 12, "bold")
F_SM     = ("Helvetica", 9)

def make_card(parent, **kw):
    return tk.Frame(parent, bg=CARD,
                    highlightbackground=BORDER, highlightthickness=1, **kw)

def label(parent, text="", fg=TEXT, font=F_MONO, bg=None, **kw):
    return tk.Label(parent, text=text, fg=fg,
                    font=font, bg=bg or parent["bg"], **kw)

def btn(parent, text, cmd, bg=ACCENT, fg=BG, font=F_MONO_B, **kw):
    return tk.Button(parent, text=text, command=cmd, bg=bg, fg=fg,
                     font=font, relief="flat", cursor="hand2",
                     activebackground=ACCENT_DIM, activeforeground=WHITE, **kw)

# ─── APP ──────────────────────────────────────────────────────────────────────
class FootballFlowApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("FootballFlow — Live Match Simulator v2")
        self.geometry("900x700")
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(820, 620)

        self.df              = None
        self.event_queue     = queue.Queue()
        self.simulation_running = False
        self.sim_start_time  = None
        self.speed_factor    = 18
        self.current_minute  = 0
        self.end_minute      = 90
        self.final_phase_label = "FULL TIME"
        self.events_done     = False
        self.team_names      = []          # [home_name, away_name]
        self.home_club_id    = None
        self.away_club_id    = None
        self._events_to_render  = 0
        self._events_rendered   = 0
        self._total_cards    = 0
        self._total_subs     = 0

        self._load_csv()
        self._build_header()
        self._show_input_screen()
        self._poll_queue()

    # ── CSV ───────────────────────────────────────────────────────────────────
    def _load_csv(self):
        try:
            self.df = pd.read_csv(CSV_PATH, low_memory=False)
            self.df.columns = self.df.columns.str.strip()
            self.df["minute"] = pd.to_numeric(self.df["minute"], errors="coerce")
            self.df.dropna(subset=["minute"], inplace=True)
            self.df["minute"]  = self.df["minute"].astype(int)
            self.df["game_id"] = pd.to_numeric(self.df["game_id"], errors="coerce").astype("Int64")
            # استنتاج event_type من description
            self.df["event_type"] = self.df["description"].apply(detect_event_type)
        except Exception as e:
            print(f"CSV load error: {e}")
            self.df = None

    # ── HEADER ────────────────────────────────────────────────────────────────
    def _build_header(self):
        hdr = tk.Frame(self, bg=BG)
        hdr.pack(fill="x")
        tk.Frame(hdr, bg="#00101a", height=4).pack(fill="x")
        inner = tk.Frame(hdr, bg=BG)
        inner.pack(fill="x", padx=28, pady=(10, 8))
        label(inner, "⚡ FOOTBALLFLOW", fg=ACCENT,
              font=("Helvetica", 17, "bold"), bg=BG).pack(side="left")
        label(inner, "  MATCH SIMULATOR  v2", fg=SUBTEXT,
              font=("Helvetica", 10), bg=BG).pack(side="left", pady=(4, 0))
        self.kafka_pill = label(inner, "● KAFKA", fg=SUBTEXT,
                                font=("Courier New", 8, "bold"), bg=BG)
        self.kafka_pill.pack(side="right")
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

    # ── CONTENT SWAP ──────────────────────────────────────────────────────────
    def _clear_content(self):
        if hasattr(self, "_content") and self._content:
            self._content.destroy()
        self._content = tk.Frame(self, bg=BG)
        self._content.pack(fill="both", expand=True)
        return self._content

    # ══════════════════════════════════════════════════════════════════════════
    #  SCREEN 1 — INPUT
    # ══════════════════════════════════════════════════════════════════════════
    def _show_input_screen(self):
        root = self._clear_content()
        col = tk.Frame(root, bg=BG)
        col.place(relx=0.5, rely=0.5, anchor="center")

        label(col, "🏟", font=("Helvetica", 48), bg=BG, fg="#1a3a2a").pack(pady=(0, 6))
        label(col, "SELECT A MATCH", fg=ACCENT,
              font=("Helvetica", 13, "bold"), bg=BG).pack()
        label(col, "Enter a Game ID from the enriched dataset to begin simulation",
              fg=SUBTEXT, font=F_SM, bg=BG).pack(pady=(2, 20))

        # ── input card
        card = make_card(col, padx=32, pady=28)
        card.pack(fill="x", ipadx=10)

        label(card, "GAME ID", fg=SUBTEXT, font=("Courier New", 8, "bold")).pack(anchor="w")
        row = tk.Frame(card, bg=CARD)
        row.pack(fill="x", pady=(6, 0))

        self.game_id_var = tk.StringVar()
        self.entry = tk.Entry(row, textvariable=self.game_id_var,
                              bg="#0a1525", fg=WHITE, insertbackground=ACCENT,
                              font=("Courier New", 15), relief="flat",
                              highlightthickness=1, highlightbackground=BORDER,
                              highlightcolor=ACCENT, width=22)
        self.entry.pack(side="left", ipady=10, padx=(0, 10))
        self.entry.bind("<Return>", lambda e: self._check_game())
        self.entry.focus()
        btn(row, "CHECK →", self._check_game, padx=18, pady=10).pack(side="left")

        self.status_lbl = label(card, "", fg=SUBTEXT, font=F_MONO_S)
        self.status_lbl.pack(anchor="w", pady=(10, 0))

        # ── match preview (hidden until valid)
        self.preview_frame = tk.Frame(card, bg=CARD)
        tk.Frame(self.preview_frame, bg=BORDER, height=1).pack(fill="x", pady=(14, 12))
        self.prev_teams = label(self.preview_frame, "", fg=WHITE,
                                font=("Helvetica", 15, "bold"))
        self.prev_teams.pack()
        self.prev_meta  = label(self.preview_frame, "", fg=SUBTEXT, font=F_SM)
        self.prev_meta.pack(pady=(4, 0))
        self.prev_phase = label(self.preview_frame, "", fg=ORANGE_ET,
                                font=("Courier New", 8, "bold"))
        self.prev_phase.pack(pady=(4, 0))
        # extra info: competition + stadium
        self.prev_extra = label(self.preview_frame, "", fg=SUBTEXT,
                                font=("Courier New", 8))
        self.prev_extra.pack(pady=(2, 0))

        # ── speed card
        sp_card = make_card(col, padx=32, pady=20)
        sp_card.pack(fill="x", pady=(14, 0), ipadx=10)
        label(sp_card, "SIMULATION SPEED", fg=SUBTEXT,
              font=("Courier New", 8, "bold")).pack(anchor="w")
        sp_row = tk.Frame(sp_card, bg=CARD)
        sp_row.pack(anchor="w", pady=(10, 0))

        self.speed_var = tk.StringVar(value="x18")
        desc = {"x5": "~18 min", "x10": "~9 min", "x18": "~5 min", "x30": "~3 min"}
        for key in SPEED_OPTIONS:
            f = tk.Frame(sp_row, bg=CARD2,
                         highlightbackground=BORDER, highlightthickness=1)
            f.pack(side="left", padx=(0, 8))
            tk.Radiobutton(f, text=f"{key}\n{desc[key]}",
                           variable=self.speed_var, value=key,
                           bg=CARD2, fg=TEXT, selectcolor=CARD2,
                           activebackground=ACCENT_DIM, activeforeground=WHITE,
                           font=("Courier New", 9), indicatoron=0,
                           relief="flat", padx=14, pady=8,
                           highlightthickness=0, cursor="hand2",
                           justify="center").pack()

        self.start_btn = btn(col, "▶   START SIMULATION",
                             self._start_simulation,
                             bg="#00512a", fg=GREEN_LIVE,
                             font=("Helvetica", 12, "bold"),
                             pady=14, state="disabled")
        self.start_btn.pack(fill="x", pady=(18, 0))

        # ── available game IDs hint
        if self.df is not None:
            ids = sorted(self.df["game_id"].dropna().unique().tolist())
            ids_str = "  |  ".join(str(int(i)) for i in ids[:10])
            label(col, f"Available IDs (first 10): {ids_str}",
                  fg=SUBTEXT, font=("Courier New", 7), bg=BG).pack(pady=(10, 0))

    # ── CHECK ─────────────────────────────────────────────────────────────────
    def _check_game(self):
        raw = self.game_id_var.get().strip()
        if not raw:
            self._setstatus("⚠  Enter a Game ID", YELLOW_EV)
            return
        try:
            gid = int(raw)
        except ValueError:
            self._setstatus("❌  Game ID must be numeric", "#ff4757")
            self.start_btn.config(state="disabled")
            self.preview_frame.pack_forget()
            return

        if self.df is None:
            self._setstatus("❌  CSV not loaded", "#ff4757")
            return

        mdf = self.df[self.df["game_id"] == gid]
        if mdf.empty:
            self._setstatus(f"❌  No match found for ID {gid}", "#ff4757")
            self.start_btn.config(state="disabled")
            self.preview_frame.pack_forget()
            return

        self.current_game_df = mdf.sort_values("minute").copy()
        self.end_minute, self.final_phase_label = determine_match_structure(self.current_game_df)

        # ── استخراج معلومات الفريقين من home/away club IDs
        first = self.current_game_df.iloc[0]
        self.home_club_id = int(first["home_club_id"])
        self.away_club_id = int(first["away_club_id"])

        # اسم الفريق من club_name بناءً على club_id
        home_rows = self.current_game_df[self.current_game_df["club_id"] == self.home_club_id]
        away_rows = self.current_game_df[self.current_game_df["club_id"] == self.away_club_id]

        home_name = home_rows["club_name"].iloc[0] if not home_rows.empty else f"Club {self.home_club_id}"
        away_name = away_rows["club_name"].iloc[0] if not away_rows.empty else f"Club {self.away_club_id}"
        self.team_names = [home_name, away_name]

        # ── النتيجة النهائية
        home_goals = int(first["home_club_goals"])
        away_goals = int(first["away_club_goals"])

        # ── معلومات إضافية
        competition = str(first.get("competition_name", "")).title()
        stadium     = str(first.get("stadium", ""))
        season      = str(first.get("season", ""))
        n_events    = len(mdf)
        max_min     = int(mdf["minute"].max())
        event_date  = str(first.get("event_date", ""))[:10]

        vs = f"  {home_name}  ⚡  {away_name}"
        self.prev_teams.config(text=vs)

        phase_text = ""
        if self.end_minute == 120:
            phase_text = "⏱ This match has EXTRA TIME (120')"
        self.prev_phase.config(text=phase_text)

        self.prev_meta.config(
            text=f"📅 {event_date}   |   🎯 {n_events} events   |   ⏱ {max_min}' played   |   Final: {home_goals}–{away_goals}"
        )
        self.prev_extra.config(
            text=f"🏆 {competition}   |   🏟 {stadium}   |   📆 Season {season}"
        )

        self.preview_frame.pack(fill="x")
        self._setstatus("✅  Match found — choose speed and start", GREEN_LIVE)
        self.start_btn.config(state="normal")

    def _setstatus(self, msg, color):
        self.status_lbl.config(text=msg, fg=color)

    # ══════════════════════════════════════════════════════════════════════════
    #  SCREEN 2 — LIVE SIMULATION
    # ══════════════════════════════════════════════════════════════════════════
    def _start_simulation(self):
        self.speed_factor   = SPEED_OPTIONS[self.speed_var.get()]
        self.current_minute = 0
        self.sim_start_time = time.time()
        self.simulation_running = True
        self.events_done    = False
        self._total_cards   = 0
        self._total_subs    = 0
        self._events_to_render  = 0
        self._events_rendered   = 0

        self._show_sim_screen()
        threading.Thread(target=self._run_producer, daemon=True).start()
        self._tick_timer()

    def _show_sim_screen(self):
        root  = self._clear_content()
        teams = self.team_names

        # ── SCOREBOARD
        sb = tk.Frame(root, bg=CARD2,
                      highlightbackground=BORDER, highlightthickness=1)
        sb.pack(fill="x", padx=16, pady=(12, 0))
        sb_inner = tk.Frame(sb, bg=CARD2)
        sb_inner.pack(pady=12, padx=20, fill="x")

        # home
        hc = tk.Frame(sb_inner, bg=CARD2)
        hc.pack(side="left", expand=True)
        label(hc, teams[0] if teams else "Home",
              fg=RED_TEAM, font=("Helvetica", 12, "bold")).pack()
        self.home_score_var = tk.StringVar(value="0")
        tk.Label(hc, textvariable=self.home_score_var,
                 bg=CARD2, fg=WHITE, font=("Helvetica", 42, "bold")).pack()

        # centre
        mc = tk.Frame(sb_inner, bg=CARD2)
        mc.pack(side="left", expand=True)
        self.live_dot = label(mc, "● LIVE", fg=GREEN_LIVE,
                              font=("Courier New", 8, "bold"))
        self.live_dot.pack()
        self.clock_var = tk.StringVar(value="0'")
        tk.Label(mc, textvariable=self.clock_var,
                 bg=CARD2, fg=ACCENT, font=("Helvetica", 28, "bold")).pack()
        self.phase_label_var = tk.StringVar(value="FIRST HALF")
        label(mc, "", fg=SUBTEXT, font=("Helvetica", 9),
              textvariable=self.phase_label_var).pack()

        # away
        ac = tk.Frame(sb_inner, bg=CARD2)
        ac.pack(side="left", expand=True)
        label(ac, teams[1] if len(teams) > 1 else "Away",
              fg=BLUE_TEAM, font=("Helvetica", 12, "bold")).pack()
        self.away_score_var = tk.StringVar(value="0")
        tk.Label(ac, textvariable=self.away_score_var,
                 bg=CARD2, fg=WHITE, font=("Helvetica", 42, "bold")).pack()

        # ── PROGRESS BAR
        prog_outer = tk.Frame(root, bg=BG)
        prog_outer.pack(fill="x", padx=16, pady=(8, 0))
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Stadium.Horizontal.TProgressbar",
                        troughcolor=CARD, background=ACCENT,
                        thickness=8, bordercolor=BORDER)
        self.progress = ttk.Progressbar(prog_outer, maximum=self.end_minute,
                                        mode="determinate",
                                        style="Stadium.Horizontal.TProgressbar")
        self.progress.pack(fill="x")
        min_row = tk.Frame(prog_outer, bg=BG)
        min_row.pack(fill="x")
        label(min_row, "0'",  fg=SUBTEXT, font=("Courier New", 7), bg=BG).pack(side="left")
        label(min_row, "45'", fg=SUBTEXT, font=("Courier New", 7),
              bg=BG).pack(side="left", padx=(200, 0))
        if self.end_minute == 120:
            label(min_row, "90'", fg=SUBTEXT,
                  font=("Courier New", 7), bg=BG).pack(side="left", padx=(180, 0))
        label(min_row, f"{self.end_minute}'", fg=SUBTEXT,
              font=("Courier New", 7), bg=BG).pack(side="right")

        # ── STATS ROW
        stats_row = tk.Frame(root, bg=BG)
        stats_row.pack(fill="x", padx=16, pady=(10, 0))

        def stat_card(parent, icon, lbl, var, fg=TEXT):
            f = make_card(parent, padx=10, pady=8)
            f.pack(side="left", expand=True, fill="x", padx=(0, 8))
            label(f, icon, font=("", 14)).pack()
            tk.Label(f, textvariable=var, bg=CARD, fg=fg,
                     font=("Helvetica", 20, "bold")).pack()
            label(f, lbl, fg=SUBTEXT, font=("Courier New", 7)).pack()

        self.stat_home_goals = tk.StringVar(value="0")
        self.stat_away_goals = tk.StringVar(value="0")
        self.stat_cards_var  = tk.StringVar(value="0")
        self.stat_subs_var   = tk.StringVar(value="0")

        stat_card(stats_row, "⚽", f"{teams[0] if teams else 'Home'} Goals",
                  self.stat_home_goals, RED_TEAM)
        stat_card(stats_row, "⚽", f"{teams[1] if len(teams) > 1 else 'Away'} Goals",
                  self.stat_away_goals, BLUE_TEAM)
        stat_card(stats_row, "🟨", "Cards",  self.stat_cards_var, YELLOW_EV)
        stat_card(stats_row, "🔄", "Subs",   self.stat_subs_var,  ACCENT)

        # ── DUAL FEED
        feeds_row = tk.Frame(root, bg=BG)
        feeds_row.pack(fill="both", expand=True, padx=16, pady=(10, 0))

        def make_feed(parent, team_name, color):
            c = tk.Frame(parent, bg=BG)
            c.pack(side="left", fill="both", expand=True, padx=(0, 6))
            hdr = make_card(c, padx=10, pady=6)
            hdr.pack(fill="x")
            label(hdr, f"● {team_name}", fg=color,
                  font=("Courier New", 9, "bold")).pack(anchor="w")
            fw = make_card(c)
            fw.pack(fill="both", expand=True, pady=(4, 0))
            txt = tk.Text(fw, bg=CARD, fg=TEXT, font=("Courier New", 9),
                          relief="flat", state="disabled", wrap="word",
                          padx=10, pady=8, cursor="arrow")
            sb2 = tk.Scrollbar(fw, command=txt.yview, bg=CARD,
                               troughcolor=CARD, highlightthickness=0)
            txt.config(yscrollcommand=sb2.set)
            sb2.pack(side="right", fill="y")
            txt.pack(fill="both", expand=True)
            return txt

        self.home_feed = make_feed(feeds_row, teams[0] if teams else "Home", RED_TEAM)
        self.away_feed = make_feed(feeds_row, teams[1] if len(teams) > 1 else "Away", BLUE_TEAM)

        # ── STATUS BAR
        self.kafka_label = label(root, "⟳ Connecting to Kafka...",
                                 fg=SUBTEXT, font=("Courier New", 8), bg=BG)
        self.kafka_label.pack(anchor="w", padx=18, pady=(4, 6))

    # ── LIVE CLOCK ────────────────────────────────────────────────────────────
    def _tick_timer(self):
        if not hasattr(self, "clock_var"):
            return

        elapsed_real = time.time() - self.sim_start_time
        sim_minute   = int(elapsed_real * self.speed_factor / 60)
        sim_minute   = min(sim_minute, self.end_minute)
        self.current_minute = sim_minute

        self.clock_var.set(f"{sim_minute}'")
        if hasattr(self, "progress"):
            self.progress["value"] = sim_minute

        if sim_minute <= 45:
            self.phase_label_var.set("FIRST HALF")
        elif sim_minute <= 90:
            self.phase_label_var.set("SECOND HALF")
        elif sim_minute <= 105:
            self.phase_label_var.set("⚡ EXTRA TIME — 1ST")
        else:
            self.phase_label_var.set("⚡ EXTRA TIME — 2ND")

        if int(elapsed_real * 2) % 2 == 0:
            self.live_dot.config(fg=GREEN_LIVE)
        else:
            self.live_dot.config(fg=CARD2)

        if (sim_minute >= self.end_minute and
                self.events_done and
                self._events_rendered >= self._events_to_render):
            self._finish_match()
            return

        self.after(200, self._tick_timer)

    # ── FINISH MATCH ──────────────────────────────────────────────────────────
    def _finish_match(self):
        self.simulation_running = False
        self._show_fulltime_banner(self.final_phase_label)

    def _show_fulltime_banner(self, text):
        if hasattr(self, "live_dot"):
            self.live_dot.config(text=f"● {text}", fg=YELLOW_EV)
        if hasattr(self, "clock_var"):
            self.clock_var.set(f"{self.end_minute}'")
        if hasattr(self, "kafka_label"):
            self.kafka_label.config(
                text=f"✅ {text} — Match complete. All events sent to Kafka.",
                fg=GREEN_LIVE)
        banner = f"\n  🏁 {text}\n"
        for feed in [self.home_feed, self.away_feed]:
            feed.config(state="normal")
            feed.insert("end", banner)
            feed.config(state="disabled")

    # ── PRODUCER THREAD ───────────────────────────────────────────────────────
    def _run_producer(self):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            self._producer = producer
            self.event_queue.put(("kafka_ok",))
        except Exception as e:
            self.event_queue.put(("kafka_err", str(e)))
            return

        df = self.current_game_df.sort_values("minute")
        self._events_to_render = len(df)
        last = -1

        for _, row in df.iterrows():
            if not self.simulation_running and not self.events_done:
                break
            cur = int(row["minute"])
            if last >= 0 and cur > last:
                time.sleep((cur - last) * 60 / self.speed_factor)
            last = cur

            # بناء الـ message بكل الـ 35 column + metadata
            msg = {}
            for col in df.columns:
                val = row.get(col)
                if pd.isna(val):
                    msg[col] = None
                elif isinstance(val, (int, float)):
                    msg[col] = val
                else:
                    msg[col] = str(val)

            # ensure types
            msg["minute"]       = cur
            msg["game_id"]      = int(row["game_id"])
            msg["club_id"]      = int(row["club_id"]) if pd.notna(row.get("club_id")) else None
            msg["home_club_id"] = int(row["home_club_id"]) if pd.notna(row.get("home_club_id")) else None
            msg["away_club_id"] = int(row["away_club_id"]) if pd.notna(row.get("away_club_id")) else None

            # metadata
            msg["_ingested_at"] = datetime.utcnow().isoformat()
            msg["_source"]      = "match_simulator_v2"
            msg["_pipeline"]    = "footballflow_streaming"

            try:
                self.event_queue.put(("event", msg))
            except Exception as e:
                self.event_queue.put(("kafka_err", str(e)))

        self.event_queue.put(("events_done",))

    # ── POLL QUEUE ────────────────────────────────────────────────────────────
    def _poll_queue(self):
        try:
            while True:
                item = self.event_queue.get_nowait()
                kind = item[0]

                if kind == "kafka_ok":
                    self.kafka_pill.config(fg=GREEN_LIVE)
                    if hasattr(self, "kafka_label"):
                        self.kafka_label.config(
                            text="✅ Connected to Kafka — streaming live",
                            fg=GREEN_LIVE)

                elif kind == "kafka_err":
                    self.kafka_pill.config(fg="#ff4757")
                    if hasattr(self, "kafka_label"):
                        self.kafka_label.config(
                            text=f"❌ Kafka: {item[1]}", fg="#ff4757")

                elif kind == "event":
                    self._render_event(item[1])

                elif kind == "events_done":
                    self.events_done = True

        except queue.Empty:
            pass
        self.after(50, self._poll_queue)

    # ── RENDER EVENT ──────────────────────────────────────────────────────────
    def _render_event(self, msg):
        minute = msg.get("minute", 0)

        # انتظر لحد ما الساعة توصل للدقيقة دي
        if minute > self.current_minute:
            self.after(50, lambda m=msg: self._render_event(m))
            return

        etype      = str(msg.get("event_type", "substitutions"))
        club_id    = msg.get("club_id")
        club_name  = str(msg.get("club_name", ""))
        desc       = str(msg.get("description") or "")[:60]
        player     = str(msg.get("player_name") or "")
        teams      = self.team_names

        # تحديد الفريق من club_id
        try:
            is_away = (club_id is not None and int(club_id) == self.away_club_id)
        except (ValueError, TypeError):
            is_away = (len(teams) > 1 and club_name == teams[1])

        if is_away:
            feed      = self.away_feed
            score_var = self.away_score_var
            goals_var = self.stat_away_goals
        else:
            feed      = self.home_feed
            score_var = self.home_score_var
            goals_var = self.stat_home_goals

        # أيقونة + إحصائيات
        if etype == "goals":
            icon = "⚽"
            cur  = int(score_var.get()) + 1
            score_var.set(str(cur))
            goals_var.set(str(cur))
        elif etype == "cards":
            desc_low = desc.lower()
            icon = "🟥" if ("red" in desc_low or "second yellow" in desc_low) else "🟨"
            self._total_cards += 1
            self.stat_cards_var.set(str(self._total_cards))
        else:
            icon = "🔄"
            self._total_subs += 1
            self.stat_subs_var.set(str(self._total_subs))

        # بناء السطر في الـ feed
        player_part = f" {player}" if player and player != "nan" else ""
        line = f" {icon} {minute:>3}'{player_part}  —  {desc}\n"

        feed.config(state="normal")
        feed.insert("end", line)
        feed.see("end")
        feed.config(state="disabled")
        self._events_rendered += 1

        # إرسال لـ Kafka
        if hasattr(self, "_producer"):
            threading.Thread(
                target=lambda m=msg: self._producer.send(KAFKA_TOPIC, value=m),
                daemon=True
            ).start()


# ─── RUN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = FootballFlowApp()
    app.mainloop()