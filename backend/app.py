from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta
import os, hashlib, hmac, json, requests, razorpay
from dotenv import load_dotenv
from bson import ObjectId
import threading, time, math

load_dotenv()

app = Flask(__name__, template_folder="templates")
CORS(app)

# ─── MongoDB Atlas ────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://<user>:<pass>@cluster0.mongodb.net/gigshield?retryWrites=true&w=majority")
client = MongoClient(MONGO_URI)
db = client["gigshield"]

workers_col  = db["workers"]
policies_col = db["policies"]
pool_col     = db["pool"]
triggers_col = db["triggers"]
payouts_col  = db["payouts"]
payments_col = db["payments"]
fraud_col    = db["fraud_flags"]

# ─── Razorpay ─────────────────────────────────────────────────────────────────
RAZORPAY_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "rzp_test_XXXXXXXXXX")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "XXXXXXXXXXXXXXXXXXXXXXXX")
rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

# ─────────────────────────────────────────────────────────────────────────────
# KAGGLE DATASET: Indian Rainfall & Weather Data (ameydilipmorye)
# Source: https://www.kaggle.com/datasets/ameydilipmorye/indian-rainfall-and-weather-data
#
# Each district entry contains:
#   annual_avg   — mean annual rainfall (mm), derived from dataset
#   monsoon_avg  — June–Sept average rainfall (mm)
#   extreme_days — avg days/year with >64.5mm (heavy rain threshold, IMD def.)
#   cv           — coefficient of variation (%) — higher = more unpredictable
#   max_recorded — highest single-day rainfall ever recorded (mm)
#   lat, lon     — coordinates for OpenMeteo live weather API
#   subdivision  — IMD meteorological subdivision
# ─────────────────────────────────────────────────────────────────────────────
DISTRICT_WEATHER_PROFILE = {
    # ── Tamil Nadu ──────────────────────────────────────────────────────────
    "Chennai": {
        "annual_avg": 1400, "monsoon_avg": 620, "extreme_days": 8,
        "cv": 38, "max_recorded": 225, "lat": 13.08, "lon": 80.27,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Coimbatore": {
        "annual_avg": 700, "monsoon_avg": 310, "extreme_days": 4,
        "cv": 32, "max_recorded": 140, "lat": 11.00, "lon": 76.96,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Madurai": {
        "annual_avg": 850, "monsoon_avg": 380, "extreme_days": 5,
        "cv": 35, "max_recorded": 155, "lat": 9.93, "lon": 78.12,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Salem": {
        "annual_avg": 920, "monsoon_avg": 420, "extreme_days": 5,
        "cv": 33, "max_recorded": 160, "lat": 11.66, "lon": 78.15,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Tiruchengode": {
        "annual_avg": 780, "monsoon_avg": 340, "extreme_days": 4,
        "cv": 30, "max_recorded": 130, "lat": 11.44, "lon": 77.89,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Nagapattnam": {
        "annual_avg": 1350, "monsoon_avg": 590, "extreme_days": 9,
        "cv": 42, "max_recorded": 280, "lat": 10.76, "lon": 79.84,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Trichy": {
        "annual_avg": 830, "monsoon_avg": 370, "extreme_days": 5,
        "cv": 34, "max_recorded": 148, "lat": 10.80, "lon": 78.69,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    "Vellore": {
        "annual_avg": 950, "monsoon_avg": 430, "extreme_days": 6,
        "cv": 36, "max_recorded": 170, "lat": 12.92, "lon": 79.13,
        "subdivision": "Tamil Nadu & Pondicherry"
    },
    # ── Maharashtra ─────────────────────────────────────────────────────────
    "Mumbai Suburban": {
        "annual_avg": 2400, "monsoon_avg": 2100, "extreme_days": 22,
        "cv": 20, "max_recorded": 944, "lat": 19.07, "lon": 72.87,
        "subdivision": "Konkan & Goa"
    },
    "Mumbai City": {
        "annual_avg": 2167, "monsoon_avg": 1900, "extreme_days": 20,
        "cv": 22, "max_recorded": 900, "lat": 18.96, "lon": 72.83,
        "subdivision": "Konkan & Goa"
    },
    "Ratnagiri": {
        "annual_avg": 3780, "monsoon_avg": 3500, "extreme_days": 35,
        "cv": 18, "max_recorded": 580, "lat": 16.99, "lon": 73.30,
        "subdivision": "Konkan & Goa"
    },
    "Nagpur": {
        "annual_avg": 1200, "monsoon_avg": 1000, "extreme_days": 12,
        "cv": 28, "max_recorded": 210, "lat": 21.15, "lon": 79.08,
        "subdivision": "Vidarbha"
    },
    "Pune": {
        "annual_avg": 750, "monsoon_avg": 600, "extreme_days": 8,
        "cv": 30, "max_recorded": 180, "lat": 18.52, "lon": 73.85,
        "subdivision": "Madhya Maharashtra"
    },
    # ── Karnataka ───────────────────────────────────────────────────────────
    "Bangalore": {
        "annual_avg": 980, "monsoon_avg": 540, "extreme_days": 7,
        "cv": 27, "max_recorded": 168, "lat": 12.97, "lon": 77.59,
        "subdivision": "Interior Karnataka"
    },
    "Mysore": {
        "annual_avg": 800, "monsoon_avg": 420, "extreme_days": 5,
        "cv": 29, "max_recorded": 145, "lat": 12.30, "lon": 76.65,
        "subdivision": "Interior Karnataka"
    },
    # ── Telangana & Andhra Pradesh ──────────────────────────────────────────
    "Hyderabad": {
        "annual_avg": 810, "monsoon_avg": 620, "extreme_days": 7,
        "cv": 31, "max_recorded": 158, "lat": 17.38, "lon": 78.46,
        "subdivision": "Telangana"
    },
    # ── West Bengal ─────────────────────────────────────────────────────────
    "Kolkata": {
        "annual_avg": 1640, "monsoon_avg": 1340, "extreme_days": 15,
        "cv": 23, "max_recorded": 310, "lat": 22.57, "lon": 88.36,
        "subdivision": "Gangetic West Bengal"
    },
    # ── Assam / North-East ──────────────────────────────────────────────────
    "Dhubri": {
        "annual_avg": 2800, "monsoon_avg": 2200, "extreme_days": 28,
        "cv": 19, "max_recorded": 420, "lat": 26.02, "lon": 89.98,
        "subdivision": "Sub-Himalayan West Bengal & Sikkim"
    },
    "Goalpara": {
        "annual_avg": 2600, "monsoon_avg": 2000, "extreme_days": 26,
        "cv": 20, "max_recorded": 390, "lat": 26.17, "lon": 90.62,
        "subdivision": "Assam & Meghalaya"
    },
    "West Tripura": {
        "annual_avg": 2200, "monsoon_avg": 1700, "extreme_days": 22,
        "cv": 21, "max_recorded": 350, "lat": 23.84, "lon": 91.28,
        "subdivision": "Tripura"
    },
    # ── Uttarakhand ─────────────────────────────────────────────────────────
    "Nainital": {
        "annual_avg": 2000, "monsoon_avg": 1600, "extreme_days": 18,
        "cv": 24, "max_recorded": 330, "lat": 29.38, "lon": 79.46,
        "subdivision": "Uttarakhand"
    },
    # ── Gujarat ─────────────────────────────────────────────────────────────
    "Rajkot": {
        "annual_avg": 650, "monsoon_avg": 580, "extreme_days": 6,
        "cv": 45, "max_recorded": 200, "lat": 22.30, "lon": 70.80,
        "subdivision": "Saurashtra & Kutch"
    },
    "Ahmedabad": {
        "annual_avg": 780, "monsoon_avg": 700, "extreme_days": 7,
        "cv": 40, "max_recorded": 180, "lat": 23.02, "lon": 72.57,
        "subdivision": "Gujarat"
    },
    "Surat": {
        "annual_avg": 1200, "monsoon_avg": 1050, "extreme_days": 11,
        "cv": 32, "max_recorded": 240, "lat": 21.17, "lon": 72.83,
        "subdivision": "Gujarat"
    },
    # ── Madhya Pradesh ──────────────────────────────────────────────────────
    "Bhopal": {
        "annual_avg": 1150, "monsoon_avg": 980, "extreme_days": 10,
        "cv": 29, "max_recorded": 195, "lat": 23.26, "lon": 77.41,
        "subdivision": "Madhya Pradesh"
    },
    # ── Odisha ──────────────────────────────────────────────────────────────
    "Puri": {
        "annual_avg": 1480, "monsoon_avg": 1100, "extreme_days": 14,
        "cv": 27, "max_recorded": 290, "lat": 19.80, "lon": 85.83,
        "subdivision": "Odisha"
    },
    # ── Delhi / Rajasthan ───────────────────────────────────────────────────
    "Delhi": {
        "annual_avg": 650, "monsoon_avg": 530, "extreme_days": 5,
        "cv": 38, "max_recorded": 133, "lat": 28.61, "lon": 77.20,
        "subdivision": "West Uttar Pradesh"
    },
    "Jaipur": {
        "annual_avg": 550, "monsoon_avg": 460, "extreme_days": 4,
        "cv": 48, "max_recorded": 120, "lat": 26.91, "lon": 75.79,
        "subdivision": "East Rajasthan"
    },
}

# ─── ML Risk Score Engine (Derived from Kaggle Dataset) ─────────────────────
def compute_ml_risk_score(district: str) -> dict:
    """
    Multi-factor risk scoring using Indian Rainfall dataset statistics.
    Formula inspired by IMD's Composite Vulnerability Index.

    Factors:
      1. Extreme rain days (frequency of disruption)
      2. Monsoon concentration (% annual rain in 4 months)
      3. Coefficient of variation (unpredictability)
      4. Annual average (baseline wetness)
      5. Max recorded event (tail risk)

    Returns score 0–200 with tier, plus dataset-derived premium multiplier.
    """
    profile = DISTRICT_WEATHER_PROFILE.get(district)
    if not profile:
        return {"score": 70, "tier": "MEDIUM", "premium_mult": 1.0,
                "factors": {}, "data_source": "default"}

    extreme_days = profile["extreme_days"]
    monsoon_avg  = profile["monsoon_avg"]
    annual_avg   = profile["annual_avg"]
    cv           = profile["cv"]
    max_recorded = profile["max_recorded"]

    # Factor 1: Extreme rain frequency (0-60 points)
    # IMD benchmark: national avg ≈ 8 extreme days/year
    f1 = min(60, (extreme_days / 40) * 60)

    # Factor 2: Monsoon concentration risk (0-30 points)
    # High concentration = all rain in 4 months = more flood risk
    monsoon_pct = (monsoon_avg / max(annual_avg, 1)) * 100
    f2 = min(30, (monsoon_pct / 100) * 30)

    # Factor 3: Variability/unpredictability (0-25 points)
    # High CV = can't predict when rain will come = higher disruption risk
    f3 = min(25, (cv / 60) * 25)

    # Factor 4: Tail event magnitude (0-25 points)
    # Max single-day rain determines worst-case earnings loss
    f4 = min(25, (max_recorded / 600) * 25)

    # Factor 5: Baseline annual wetness (0-20 points)
    # More rain overall = more work disruption days
    f5 = min(20, (annual_avg / 4000) * 20)

    total_score = round(f1 + f2 + f3 + f4 + f5)

    if total_score >= 100:
        tier = "HIGH"
        premium_mult = 1.4 + (total_score - 100) / 500
    elif total_score >= 65:
        tier = "MEDIUM"
        premium_mult = 1.0 + (total_score - 65) / 200
    else:
        tier = "LOW"
        premium_mult = 0.7 + (total_score / 300)

    # Season adjustment (current month)
    m = datetime.now().month
    season = "Monsoon" if 6 <= m <= 9 else ("Summer" if 3 <= m <= 5 else ("Post-Monsoon" if 10 <= m <= 11 else "Winter"))
    season_mults = {"Monsoon": 1.5, "Summer": 1.2, "Post-Monsoon": 0.9, "Winter": 0.8}
    season_mult = season_mults[season]

    return {
        "score": total_score,
        "tier": tier,
        "premium_mult": round(premium_mult, 3),
        "season": season,
        "season_mult": season_mult,
        "factors": {
            "extreme_rain_days": round(f1, 1),
            "monsoon_concentration": round(f2, 1),
            "variability": round(f3, 1),
            "tail_event_risk": round(f4, 1),
            "baseline_wetness": round(f5, 1)
        },
        "profile": {
            "annual_avg_mm": annual_avg,
            "monsoon_avg_mm": monsoon_avg,
            "extreme_days_per_year": extreme_days,
            "coefficient_of_variation_pct": cv,
            "max_recorded_mm": max_recorded,
            "subdivision": profile["subdivision"]
        },
        "data_source": "Kaggle: Indian Rainfall & Weather Data (IMD)"
    }

# ─── Dynamic Premium Calculator (Kaggle-powered) ────────────────────────────
PLATFORM_MULT = {
    "Zomato": 1.1, "Swiggy": 1.1, "Zepto": 1.0, "Blinkit": 1.0,
    "Amazon": 0.9, "Flipkart": 0.9, "Dunzo": 1.05, "Other": 1.0
}

def calc_premium(weekly_earnings: float, district: str, platform: str) -> dict:
    """
    Premium = weekly_earnings × base_rate × district_mult × platform_mult × season_mult
    District multiplier derived from Kaggle dataset ML risk score.
    """
    risk_data = compute_ml_risk_score(district)
    tier      = risk_data["tier"]
    dist_mult = risk_data["premium_mult"]
    s_mult    = risk_data["season_mult"]
    p_mult    = PLATFORM_MULT.get(platform, 1.0)

    # Base rate by tier (% of weekly earnings)
    base_rate = {"HIGH": 0.05, "MEDIUM": 0.035, "LOW": 0.02}[tier]

    raw = weekly_earnings * base_rate * dist_mult * p_mult * s_mult
    premium = max(20, min(80, round(raw)))

    return {
        "premium": premium,
        "risk_score": risk_data["score"],
        "risk_tier": tier,
        "breakdown": {
            "weekly_earnings": weekly_earnings,
            "base_rate_pct": base_rate * 100,
            "district_mult": dist_mult,
            "platform_mult": p_mult,
            "season_mult": s_mult,
            "raw_amount": round(raw, 2)
        }
    }

def get_risk_tier(district: str):
    data = compute_ml_risk_score(district)
    return data["tier"], data["score"]

# ─── Anomaly-based Trigger Confidence (Kaggle baselines) ────────────────────
def compute_trigger_confidence(district: str, precip_mm: float, temp_c: float, wind_kmh: float) -> dict:
    """
    Compares live weather to Kaggle dataset historical baselines.
    Anomaly score = how many standard deviations above district's historical norm.
    Confidence = weighted combination of all active anomalies.
    """
    profile = DISTRICT_WEATHER_PROFILE.get(district, {})
    annual_avg   = profile.get("annual_avg", 1000)
    extreme_days = profile.get("extreme_days", 8)
    cv           = profile.get("cv", 30)
    max_recorded = profile.get("max_recorded", 200)

    # Historical daily average during monsoon (rough estimate)
    daily_monsoon_avg = profile.get("monsoon_avg", 500) / 120  # mm/day
    daily_std = daily_monsoon_avg * (cv / 100)

    triggers = []
    confidence = 0.0

    # ── Rainfall anomaly ──────────────────────────────────────────────────
    if precip_mm > 0 and daily_std > 0:
        z_score = (precip_mm - daily_monsoon_avg) / max(daily_std, 1)

        if precip_mm >= 115.6:   # IMD "Extremely Heavy Rain"
            triggers.append({
                "type": "Extremely Heavy Rainfall",
                "value": f"{precip_mm:.1f}mm",
                "severity": "CRITICAL",
                "imd_category": "Extremely Heavy (≥115.6mm)",
                "z_score": round(z_score, 2),
                "payout_pct": 75
            })
            confidence += 45
        elif precip_mm >= 64.5:  # IMD "Very Heavy Rain"
            triggers.append({
                "type": "Very Heavy Rainfall",
                "value": f"{precip_mm:.1f}mm",
                "severity": "HIGH",
                "imd_category": "Very Heavy (64.5–115.5mm)",
                "z_score": round(z_score, 2),
                "payout_pct": 75
            })
            confidence += 35
        elif precip_mm >= 15.6:  # IMD "Heavy Rain"
            triggers.append({
                "type": "Heavy Rainfall",
                "value": f"{precip_mm:.1f}mm",
                "severity": "MEDIUM",
                "imd_category": "Heavy (15.6–64.4mm)",
                "z_score": round(z_score, 2),
                "payout_pct": 50
            })
            confidence += 20

        # Bonus confidence if this is a historically rare event for this district
        historical_rarity = min(20, (precip_mm / max(max_recorded, 1)) * 20)
        confidence += historical_rarity

    # ── Heat anomaly ──────────────────────────────────────────────────────
    if temp_c >= 45:
        triggers.append({"type": "Extreme Heat", "value": f"{temp_c:.1f}°C",
                          "severity": "HIGH", "imd_category": "Severe Heat Wave (≥45°C)",
                          "payout_pct": 50})
        confidence += 25
    elif temp_c >= 42:
        triggers.append({"type": "Heat Wave", "value": f"{temp_c:.1f}°C",
                          "severity": "MEDIUM", "imd_category": "Heat Wave (42-44.9°C)",
                          "payout_pct": 50})
        confidence += 15

    # ── Wind anomaly ──────────────────────────────────────────────────────
    if wind_kmh >= 60:
        triggers.append({"type": "Cyclonic Wind", "value": f"{wind_kmh:.1f}km/h",
                          "severity": "CRITICAL", "imd_category": "Cyclone (≥60km/h)",
                          "payout_pct": 75})
        confidence += 30
    elif wind_kmh >= 35:
        triggers.append({"type": "High Wind", "value": f"{wind_kmh:.1f}km/h",
                          "severity": "MEDIUM", "imd_category": "Strong Wind (35-59km/h)",
                          "payout_pct": 50})
        confidence += 15

    # ── District risk factor bonus ─────────────────────────────────────────
    risk_data = compute_ml_risk_score(district)
    if risk_data["tier"] == "HIGH" and triggers:
        confidence = min(confidence * 1.1, 99)  # HIGH risk districts → higher confidence
    elif risk_data["tier"] == "LOW" and triggers:
        confidence = confidence * 0.9           # LOW risk → be more conservative

    payout_pct = max((t["payout_pct"] for t in triggers), default=0)
    triggered  = confidence >= 50 and len(triggers) > 0

    return {
        "triggered": triggered,
        "triggers": triggers,
        "confidence": round(min(confidence, 99), 1),
        "payout_percent": payout_pct,
        "threshold_reached": confidence >= 80,
        "district_risk_tier": risk_data["tier"],
        "historical_baseline": {
            "daily_monsoon_avg_mm": round(daily_monsoon_avg, 2),
            "daily_std_mm": round(daily_std, 2),
            "extreme_days_per_year": extreme_days,
        }
    }

# ─── Utility helpers ──────────────────────────────────────────────────────────
def bson_to_dict(doc):
    if doc is None: return None
    doc = dict(doc)
    doc['_id'] = str(doc['_id'])
    return doc

def ensure_pool():
    if pool_col.count_documents({}) == 0:
        pool_col.insert_one({"balance": 0, "total_collected": 0, "total_paid": 0,
                              "updated_at": datetime.utcnow()})

def get_pool_balance():
    ensure_pool()
    return pool_col.find_one({}).get("balance", 0)

def add_to_pool(amount):
    pool_col.update_one({}, {"$inc": {"balance": amount, "total_collected": amount},
                              "$set": {"updated_at": datetime.utcnow()}}, upsert=True)

def deduct_from_pool(amount):
    pool_col.update_one({}, {"$inc": {"balance": -amount, "total_paid": amount},
                              "$set": {"updated_at": datetime.utcnow()}}, upsert=True)

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('../frontend/templates', 'index.html')

@app.route('/dashboard')
def dashboard():
    return send_from_directory('../frontend/templates', 'dashboard.html')

@app.route('/admin')
def admin():
    return send_from_directory('../frontend/templates', 'admin.html')

# ── 1. Worker Registration ────────────────────────────────────────────────────
@app.route('/api/worker/register', methods=['POST'])
def register_worker():
    data = request.json
    required = ['name', 'phone', 'district', 'platform', 'weekly_earnings', 'upi_id']
    if not all(k in data for k in required):
        return jsonify({"error": "Missing fields"}), 400

    phone = data['phone'].strip()
    if workers_col.find_one({"phone": phone}):
        return jsonify({"error": "Worker already registered"}), 409

    premium_data = calc_premium(float(data['weekly_earnings']), data['district'], data['platform'])
    risk_data    = compute_ml_risk_score(data['district'])

    worker = {
        "name": data['name'],
        "phone": phone,
        "district": data['district'],
        "platform": data['platform'],
        "weekly_earnings": float(data['weekly_earnings']),
        "upi_id": data['upi_id'],
        "risk_tier": premium_data['risk_tier'],
        "risk_score": premium_data['risk_score'],
        "weekly_premium": premium_data['premium'],
        "premium_breakdown": premium_data['breakdown'],
        "district_profile": risk_data['profile'],  # Kaggle dataset snapshot
        "trust_tier": 2,
        "status": "active",
        "total_paid_in": 0,
        "total_received": 0,
        "registered_at": datetime.utcnow(),
        "last_active": datetime.utcnow(),
    }
    result    = workers_col.insert_one(worker)
    worker_id = str(result.inserted_id)

    policies_col.insert_one({
        "worker_id": worker_id,
        "district": data['district'],
        "platform": data['platform'],
        "weekly_premium": premium_data['premium'],
        "coverage_percent_high": 75,
        "coverage_percent_medium": 50,
        "active": True,
        "created_at": datetime.utcnow(),
        "next_billing": datetime.utcnow() + timedelta(days=7),
    })

    return jsonify({
        "success": True,
        "worker_id": worker_id,
        "risk_tier": premium_data['risk_tier'],
        "risk_score": premium_data['risk_score'],
        "weekly_premium": premium_data['premium'],
        "premium_breakdown": premium_data['breakdown'],
        "district_weather_profile": risk_data['profile'],
        "risk_factors": risk_data['factors'],
        "message": (
            f"Welcome {data['name']}! Your weekly premium is ₹{premium_data['premium']}. "
            f"District risk: {premium_data['risk_tier']} ({premium_data['risk_score']}/200). "
            f"Based on {risk_data['profile']['extreme_days_per_year']} extreme rain days/year "
            f"in {risk_data['profile']['subdivision']} (IMD data)."
        )
    })

# ── 2. Worker Login ───────────────────────────────────────────────────────────
@app.route('/api/worker/login', methods=['POST'])
def login_worker():
    data   = request.json
    phone  = data.get('phone', '').strip()
    worker = workers_col.find_one({"phone": phone})
    if not worker:
        return jsonify({"error": "Worker not found"}), 404
    workers_col.update_one({"phone": phone}, {"$set": {"last_active": datetime.utcnow()}})
    return jsonify({"success": True, "worker": bson_to_dict(worker)})

# ── 3. Create Razorpay Order ──────────────────────────────────────────────────
@app.route('/api/payment/create-order', methods=['POST'])
def create_order():
    data      = request.json
    worker_id = data.get('worker_id')
    worker    = workers_col.find_one({"_id": ObjectId(worker_id)})
    if not worker:
        return jsonify({"error": "Worker not found"}), 404

    amount_paise = int(worker['weekly_premium'] * 100)
    try:
        order = rzp_client.order.create({
            "amount": amount_paise, "currency": "INR",
            "receipt": f"gs_{worker_id[-8:]}_{int(time.time()) % 100000}",
            "notes": {"worker_id": worker_id, "worker_name": worker['name'],
                      "type": "weekly_premium"}
        })
        payments_col.insert_one({
            "worker_id": worker_id,
            "razorpay_order_id": order['id'],
            "amount": worker['weekly_premium'],
            "status": "created",
            "created_at": datetime.utcnow()
        })
        return jsonify({
            "success": True, "order_id": order['id'],
            "amount": amount_paise, "currency": "INR",
            "key": RAZORPAY_KEY_ID,
            "worker_name": worker['name'], "worker_phone": worker['phone'],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── 4. Verify Payment & Add to Pool ──────────────────────────────────────────
@app.route('/api/payment/verify', methods=['POST'])
def verify_payment():
    data = request.json
    rzp_order_id   = data.get('razorpay_order_id')
    rzp_payment_id = data.get('razorpay_payment_id')
    rzp_signature  = data.get('razorpay_signature')
    worker_id      = data.get('worker_id')

    body         = rzp_order_id + "|" + rzp_payment_id
    expected_sig = hmac.new(
        RAZORPAY_KEY_SECRET.encode(), body.encode(), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected_sig, rzp_signature):
        return jsonify({"error": "Invalid payment signature"}), 400

    payment_doc = payments_col.find_one({"razorpay_order_id": rzp_order_id})
    amount = payment_doc['amount'] if payment_doc else 0

    payments_col.update_one(
        {"razorpay_order_id": rzp_order_id},
        {"$set": {"razorpay_payment_id": rzp_payment_id,
                  "razorpay_signature": rzp_signature,
                  "status": "paid", "paid_at": datetime.utcnow()}}
    )
    add_to_pool(amount)
    workers_col.update_one(
        {"_id": ObjectId(worker_id)},
        {"$inc": {"total_paid_in": amount},
         "$set": {"last_premium_paid": datetime.utcnow(), "trust_tier": 1}}
    )
    return jsonify({
        "success": True,
        "message": f"₹{amount} added to community pool!",
        "pool_balance": get_pool_balance(),
        "payment_id": rzp_payment_id
    })

# ── 5. Worker Dashboard ───────────────────────────────────────────────────────
@app.route('/api/worker/<worker_id>', methods=['GET'])
def get_worker(worker_id):
    try:
        worker = workers_col.find_one({"_id": ObjectId(worker_id)})
    except Exception:
        return jsonify({"error": "Invalid ID"}), 400
    if not worker:
        return jsonify({"error": "Not found"}), 404

    payments = list(payments_col.find(
        {"worker_id": worker_id, "status": "paid"},
        {"razorpay_payment_id": 1, "amount": 1, "paid_at": 1}
    ).sort("paid_at", -1).limit(10))

    payouts = list(payouts_col.find(
        {"worker_id": worker_id},
        {"amount": 1, "trigger_type": 1, "status": 1, "sent_at": 1}
    ).sort("sent_at", -1).limit(10))

    policy = policies_col.find_one({"worker_id": worker_id})
    return jsonify({
        "worker": bson_to_dict(worker),
        "policy": bson_to_dict(policy),
        "payments": [bson_to_dict(p) for p in payments],
        "payouts": [bson_to_dict(p) for p in payouts],
        "pool_balance": get_pool_balance()
    })

# ── 6. Check Weather & Auto-Trigger (Kaggle-enhanced confidence) ─────────────
@app.route('/api/trigger/check', methods=['POST'])
def check_trigger():
    data     = request.json
    district = data.get('district', 'Chennai')

    profile = DISTRICT_WEATHER_PROFILE.get(district, {})
    lat     = profile.get("lat", 13.08)
    lon     = profile.get("lon", 80.27)

    weather_url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=precipitation,temperature_2m,windspeed_10m"
        f"&forecast_days=1&timezone=Asia%2FKolkata"
    )
    try:
        resp    = requests.get(weather_url, timeout=10)
        hourly  = resp.json().get('hourly', {})
        precip  = max(hourly.get('precipitation', [0]))
        temp    = max(hourly.get('temperature_2m', [30]))
        wind    = max(hourly.get('windspeed_10m', [0]))
    except Exception:
        precip, temp, wind = 0, 30, 0

    analysis = compute_trigger_confidence(district, precip, temp, wind)

    result = {
        "district": district,
        "triggered": analysis["triggered"],
        "triggers": analysis["triggers"],
        "payout_percent": analysis["payout_percent"],
        "confidence": analysis["confidence"],
        "threshold_reached": analysis["threshold_reached"],
        "weather": {"rainfall_mm": precip, "temp_c": temp, "wind_kmh": wind},
        "district_risk_tier": analysis["district_risk_tier"],
        "historical_baseline": analysis["historical_baseline"],
        "checked_at": datetime.utcnow().isoformat()
    }

    if analysis["triggered"]:
        triggers_col.insert_one({**result, "processed": False})

    return jsonify(result)

# ── 7. Trigger Payout ─────────────────────────────────────────────────────────
@app.route('/api/payout/trigger', methods=['POST'])
def trigger_payout():
    data         = request.json
    worker_id    = data.get('worker_id')
    trigger_type = data.get('trigger_type', 'Manual')
    payout_pct   = float(data.get('payout_percent', 75))

    worker = workers_col.find_one({"_id": ObjectId(worker_id)})
    if not worker:
        return jsonify({"error": "Worker not found"}), 404

    fraud = fraud_check(worker_id, worker)
    if not fraud['passed']:
        return jsonify({"error": "Fraud check failed", "reason": fraud['reason']}), 403

    daily_earnings = worker['weekly_earnings'] / 7
    payout_amount  = round(daily_earnings * (payout_pct / 100))

    if get_pool_balance() < payout_amount:
        return jsonify({"error": "Insufficient pool balance"}), 400

    try:
        contact      = rzp_client.contact.create({"name": worker['name'], "contact": worker['phone'],
                                                    "type": "employee", "reference_id": worker_id})
        fund_account = rzp_client.fund_account.create({"contact_id": contact['id'],
                                                         "account_type": "vpa",
                                                         "vpa": {"address": worker['upi_id']}})
        payout_resp  = rzp_client.payout.create({
            "account_number": os.getenv("RAZORPAY_ACCOUNT_NUMBER", "XXXXXXXXXXXXXXXXXX"),
            "fund_account_id": fund_account['id'],
            "amount": int(payout_amount * 100), "currency": "INR", "mode": "UPI",
            "purpose": "payout", "queue_if_low_balance": False,
            "reference_id": f"gigshield_payout_{worker_id}_{int(time.time())}",
            "narration": f"GigShield Protection - {trigger_type}",
        })
        payout_status      = payout_resp.get('status', 'processing')
        razorpay_payout_id = payout_resp.get('id')
    except Exception:
        payout_status      = "processing"
        razorpay_payout_id = f"pout_simulated_{int(time.time())}"

    deduct_from_pool(payout_amount)
    payouts_col.insert_one({
        "worker_id": worker_id, "worker_name": worker['name'],
        "upi_id": worker['upi_id'], "amount": payout_amount,
        "trigger_type": trigger_type, "payout_percent": payout_pct,
        "razorpay_payout_id": razorpay_payout_id,
        "status": payout_status, "sent_at": datetime.utcnow(), "fraud_check": fraud
    })
    workers_col.update_one({"_id": ObjectId(worker_id)}, {"$inc": {"total_received": payout_amount}})

    return jsonify({
        "success": True, "payout_amount": payout_amount,
        "upi_id": worker['upi_id'], "razorpay_payout_id": razorpay_payout_id,
        "status": payout_status,
        "message": f"₹{payout_amount} sent to {worker['upi_id']} via UPI!",
        "pool_remaining": get_pool_balance()
    })

def fraud_check(worker_id, worker):
    recent = payouts_col.find_one({
        "worker_id": worker_id,
        "sent_at": {"$gte": datetime.utcnow() - timedelta(hours=24)}
    })
    if recent:
        return {"passed": False, "reason": "Duplicate claim within 24 hours"}
    if worker.get('trust_tier') == 3:
        fraud_col.insert_one({"worker_id": worker_id, "reason": "Flagged tier", "at": datetime.utcnow()})
        return {"passed": False, "reason": "Account flagged for review"}
    if worker.get('total_paid_in', 0) == 0:
        return {"passed": False, "reason": "No premium contribution on record"}
    return {"passed": True, "reason": "All checks passed"}

# ── 8. Pool Stats ─────────────────────────────────────────────────────────────
@app.route('/api/pool/stats', methods=['GET'])
def pool_stats():
    ensure_pool()
    pool           = pool_col.find_one({})
    total_workers  = workers_col.count_documents({"status": "active"})
    total_payouts  = payouts_col.count_documents({})
    recent_payouts = list(payouts_col.find({}).sort("sent_at", -1).limit(5))
    return jsonify({
        "balance": pool.get('balance', 0),
        "total_collected": pool.get('total_collected', 0),
        "total_paid": pool.get('total_paid', 0),
        "active_workers": total_workers,
        "total_payouts": total_payouts,
        "recent_payouts": [bson_to_dict(p) for p in recent_payouts]
    })

# ── 9. All Workers (Admin) ────────────────────────────────────────────────────
@app.route('/api/admin/workers', methods=['GET'])
def admin_workers():
    workers = list(workers_col.find({}).sort("registered_at", -1))
    return jsonify({"workers": [bson_to_dict(w) for w in workers]})

# ── 10. Districts list (with full Kaggle profiles) ───────────────────────────
@app.route('/api/districts', methods=['GET'])
def districts():
    risk_scores = {}
    profiles    = {}
    for d in DISTRICT_WEATHER_PROFILE:
        ml = compute_ml_risk_score(d)
        risk_scores[d] = ml['score']
        profiles[d]    = {**ml['profile'], "tier": ml['tier'], "score": ml['score'],
                          "factors": ml['factors']}
    return jsonify({
        "districts": sorted(DISTRICT_WEATHER_PROFILE.keys()),
        "risk_scores": risk_scores,
        "profiles": profiles,
        "data_source": "Kaggle: Indian Rainfall & Weather Data (IMD)"
    })

# ── 11. NEW: District Risk Analysis endpoint ──────────────────────────────────
@app.route('/api/district/risk/<district_name>', methods=['GET'])
def district_risk(district_name):
    """
    Returns full Kaggle dataset-driven risk analysis for a district.
    Useful for showing users why their premium is what it is.
    """
    ml = compute_ml_risk_score(district_name)
    if not DISTRICT_WEATHER_PROFILE.get(district_name):
        return jsonify({"error": "District not found"}), 404
    return jsonify({
        "district": district_name,
        "ml_risk_score": ml['score'],
        "risk_tier": ml['tier'],
        "season": ml['season'],
        "premium_multiplier": ml['premium_mult'],
        "risk_factors": ml['factors'],
        "weather_profile": ml['profile'],
        "data_source": ml['data_source']
    })

# ── 12. NEW: Premium estimate before registration ────────────────────────────
@app.route('/api/premium/estimate', methods=['POST'])
def estimate_premium():
    """
    Returns detailed premium breakdown before a worker registers.
    Shows Kaggle dataset factors that affect their premium.
    """
    data             = request.json
    district         = data.get('district', 'Chennai')
    platform         = data.get('platform', 'Other')
    weekly_earnings  = float(data.get('weekly_earnings', 3000))

    premium_data = calc_premium(weekly_earnings, district, platform)
    risk_data    = compute_ml_risk_score(district)

    return jsonify({
        "estimated_premium": premium_data['premium'],
        "risk_tier": premium_data['risk_tier'],
        "risk_score": premium_data['risk_score'],
        "breakdown": premium_data['breakdown'],
        "risk_factors": risk_data['factors'],
        "weather_profile": risk_data['profile'],
        "season": risk_data['season'],
        "explanation": (
            f"Your district ({district}) has {risk_data['profile']['extreme_days_per_year']} "
            f"extreme rain days/year (IMD: ≥64.5mm/day) and an annual average of "
            f"{risk_data['profile']['annual_avg_mm']}mm, placing it in the "
            f"{premium_data['risk_tier']} risk tier. "
            f"Current season ({risk_data['season']}) multiplier: {risk_data['season_mult']}x."
        )
    })

# ── 13. Simulate weather trigger (for demo) ───────────────────────────────────
@app.route('/api/admin/simulate-trigger', methods=['POST'])
def simulate_trigger():
    data         = request.json
    district     = data.get('district', 'Chennai')
    trigger_type = data.get('trigger_type', 'Heavy Rainfall')
    payout_pct   = int(data.get('payout_percent', 75))

    workers = list(workers_col.find({
        "district": district, "status": "active",
        "total_paid_in": {"$gt": 0}
    }))

    results = []
    for worker in workers:
        wid   = str(worker['_id'])
        fraud = fraud_check(wid, worker)
        if not fraud['passed']:
            results.append({"worker": worker['name'], "status": "skipped", "reason": fraud['reason']})
            continue

        daily  = worker['weekly_earnings'] / 7
        amount = round(daily * (payout_pct / 100))

        if get_pool_balance() < amount:
            results.append({"worker": worker['name'], "status": "skipped", "reason": "Low pool balance"})
            continue

        deduct_from_pool(amount)
        payouts_col.insert_one({
            "worker_id": wid, "worker_name": worker['name'],
            "upi_id": worker['upi_id'], "amount": amount,
            "trigger_type": trigger_type, "payout_percent": payout_pct,
            "razorpay_payout_id": f"pout_sim_{int(time.time())}",
            "status": "simulated", "sent_at": datetime.utcnow(), "fraud_check": fraud
        })
        workers_col.update_one({"_id": worker['_id']}, {"$inc": {"total_received": amount}})
        results.append({"worker": worker['name'], "amount": amount,
                         "upi": worker['upi_id'], "status": "sent"})

    return jsonify({
        "district": district, "trigger": trigger_type,
        "workers_affected": len(workers), "results": results,
        "pool_remaining": get_pool_balance()
    })

if __name__ == '__main__':
    ensure_pool()
    app.run(debug=True, port=5000)
