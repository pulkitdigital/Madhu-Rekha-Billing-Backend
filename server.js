// server.js
import express from "express";
import cors from "cors";
import "dotenv/config";
import PDFDocument from "pdfkit";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { db } from "./firebaseClient.js";
import {
  syncBillToSheet,
  syncItemsToSheet,
  syncPaymentToSheet,
  syncRefundToSheet,
  syncDeleteBillFromSheet,
  syncDeleteItemsFromSheet,
  syncDeletePaymentsFromSheet,
  syncDeleteRefundsFromSheet,
  syncProfileToSheet,
} from "./sheetIntregation.js";

// NEW: performance middlewares
import compression from "compression";
import NodeCache from "node-cache";

const app = express();
const PORT = process.env.PORT || 4000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------- CACHE SETUP ----------
const cache = new NodeCache({
  stdTTL: 60, // default 60s
  checkperiod: 120,
});

// tiny helper
function makeCacheKey(...parts) {
  return parts.join("::");
}

async function getOrSetCache(key, ttlSeconds, fetchFn) {
  const cached = cache.get(key);
  if (cached !== undefined) return cached;
  const fresh = await fetchFn();
  cache.set(key, fresh, ttlSeconds);
  return fresh;
}

// ---------- STATIC FILES (fonts, etc.) ----------
app.use("/resources", express.static(path.join(__dirname, "resources")));

// ---------- BASIC MIDDLEWARE ----------
app.use(cors());
app.use(express.json());

// NEW: enable gzip/deflate compression
app.use(compression());

// ---------- HELPERS ----------
function computeStatus(total, paidEffective) {
  if (!total || total <= 0) return "PENDING";
  if (paidEffective >= total) return "PAID";
  if (paidEffective > 0 && paidEffective < total) return "PARTIAL";
  return "PENDING";
}

// date formatting: produce DD.MM.YYYY (for any date or date-time string)
function formatDateDot(dateStrOrDate) {
  if (!dateStrOrDate) return "";
  const d =
    typeof dateStrOrDate === "string" ? new Date(dateStrOrDate) : dateStrOrDate;
  if (Number.isNaN(d.getTime && d.getTime())) {
    // try to parse common yyyy-mm-dd or iso fragments
    if (typeof dateStrOrDate === "string") {
      const s = dateStrOrDate.split("T")[0];
      if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
        const parts = s.split("-");
        return `${parts[2].padStart(2, "0")}.${parts[1].padStart(2, "0")}.${
          parts[0]
        }`;
      }
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) {
        // already dd/mm/yyyy -> convert to dots
        return s.replace(/\//g, ".");
      }
    }
    return String(dateStrOrDate);
  }
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// format date-time as DD.MM.YYYY HH:MM (used in timelines)
function formatDateTimeDot(dt) {
  if (!dt) return "";
  const d = new Date(dt);
  if (Number.isNaN(d.getTime())) return formatDateDot(dt);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yyyy = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${dd}.${mm}.${yyyy} ${hh}:${mi}`;
}

// Returns "25-26" for FY 2025-26 based on Indian FY (Apr–Mar)
function getFinancialYearCode(dateStrOrDate) {
  const d = dateStrOrDate ? new Date(dateStrOrDate) : new Date();
  let year = d.getFullYear();
  const month = d.getMonth() + 1; // 1-12

  // Indian FY: if month < April, FY starts previous year
  let fyStart = month >= 4 ? year : year - 1;
  let fyEnd = fyStart + 1;

  const fyStartShort = String(fyStart).slice(-2);
  const fyEndShort = String(fyEnd).slice(-2);
  return `${fyStartShort}-${fyEndShort}`; // e.g., "25-26"
}

function cleanFieldsByMode(mode, data) {
  const cleared = {
    chequeDate: null,
    chequeNumber: null,
    bankName: null,
    transferType: null,
    transferDate: null,
    upiName: null,
    upiId: null,
    upiDate: null,
  };

  if (mode === "Cheque") {
    delete cleared.chequeDate;
    delete cleared.chequeNumber;
    delete cleared.bankName;
  }

  if (mode === "BankTransfer") {
    delete cleared.transferType;
    delete cleared.transferDate;
    delete cleared.bankName;
  }

  if (mode === "UPI") {
    delete cleared.upiName;
    delete cleared.upiId;
    delete cleared.upiDate;
  }

  // Cash → everything stays cleared
  return { ...data, ...cleared };
}

// Generate invoice number WITHOUT counters collection
async function generateInvoiceNumber(billDateInput) {
  const dateStr = billDateInput || new Date().toISOString().slice(0, 10);
  const fy = getFinancialYearCode(dateStr);
  const prefix = `${fy}/S-`;

  const snap = await db
    .collection("bills")
    .where("invoiceNo", ">=", prefix)
    .where("invoiceNo", "<=", prefix + "\uf8ff")
    .orderBy("invoiceNo", "desc")
    .limit(1)
    .get();

  let nextNumber = 1;
  if (!snap.empty) {
    const last = snap.docs[0].data().invoiceNo || "";
    // extract trailing digits (serial) safely
    const m = last.match(/(\d+)$/);
    const current = m ? Number(m[1]) : 0;
    nextNumber = current + 1;
  }

  const serial = String(nextNumber).padStart(4, "0");
  const invoiceNo = `${fy}/S-${serial}`;

  return { invoiceNo, fy, serial };
}

// Parse "25-26/S-0001" into { fy: "25-26", invoiceSerial: "0001" }
function parseInvoiceNumber(invoiceNo) {
  const [fy, rest] = (invoiceNo || "").split("/");
  if (!fy || !rest) return { fy: "00-00", invoiceSerial: "0000" };
  // get trailing digits after last dash
  const m = rest.match(/(\d+)$/);
  const invoiceSerial = m ? String(m[1]).padStart(4, "0") : "0000";
  return { fy, invoiceSerial };
}

// Generate receipt id per invoice WITHOUT counters collection
// Uses how many payments exist for that bill.
async function generateReceiptId(invoiceNo, billId) {
  if (!billId) {
    throw new Error("billId is required for generateReceiptId");
  }

  const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);

  const snap = await db
    .collection("payments")
    .where("billId", "==", billId)
    .get();

  const seq = snap.size + 1;
  const recSerial = String(seq).padStart(4, "0");
  return `${fy}/S-${invoiceSerial}/REC-${recSerial}`;
}

// Generate refund id per invoice WITHOUT counters collection
// Uses how many refunds exist for that bill.
async function generateRefundId(invoiceNo, billId) {
  if (!billId) {
    throw new Error("billId is required for generateRefundId");
  }

  const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);

  const snap = await db
    .collection("refunds")
    .where("billId", "==", billId)
    .get();

  const seq = snap.size + 1;
  const refSerial = String(seq).padStart(4, "0");
  return `${fy}/S-${invoiceSerial}/REF-${refSerial}`;
}

// FRONTEND base URL (React app)
const FRONTEND_BASE =
  process.env.FRONTEND_BASE_URL ||
  (process.env.NODE_ENV === "production"
    ? "https://madhu-rekha-billing-software.vercel.app"
    : "http://localhost:5173");

// ---------- CLINIC PROFILE HELPER (cache by default; allow force fresh read) ----------
async function getClinicProfile({ force = false } = {}) {
  const key = makeCacheKey("profile", "clinic");
  if (force) {
    const snap = await db.collection("settings").doc("clinicProfile").get();
    const data = snap.exists ? snap.data() : null;
    if (data) cache.set(key, data, 300); // update cache for other readers
    return data;
  }
  return await getOrSetCache(key, 300, async () => {
    const snap = await db.collection("settings").doc("clinicProfile").get();
    return snap.exists ? snap.data() : null;
  });
}

// safe accessor (returns empty string if missing) — avoids 'undefined' in PDFs
function profileValue(profile, key) {
  if (!profile) return "";
  const v = profile[key];
  if (typeof v === "undefined" || v === null) return "";
  return String(v);
}

// ---------- HEALTH CHECK ----------
app.get("/", (_req, res) => {
  res.send("Backend OK");
});

//
// FIRESTORE SCHEMA (doctorReg removed):
//
// bills:
//   { patientName, sex, address, age, date, invoiceNo, total, paid, refunded, balance, status, createdAt, remarks, services: [...] }
// items: { billId, patientName, description, qty, rate, amount }
// payments: { billId, amount, mode, receiptNo, paymentDate, paymentTime, paymentDateTime, ... }
// refunds: { billId, amount, ... }
//

// ---------- GET /api/dashboard/summary ----------
app.get("/api/dashboard/summary", async (_req, res) => {
  try {
    const key = makeCacheKey("dashboard", "summary");
    const data = await getOrSetCache(key, 60, async () => {
      const now = new Date();
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, "0");
      const dd = String(now.getDate()).padStart(2, "0");

      const todayStrISO = `${yyyy}-${mm}-${dd}`; // for querying Firestore date-like strings (saved as yyyy-mm-dd)
      const todayLabel = `${dd}.${mm}.${yyyy}`;
      const monthStart = `${yyyy}-${mm}-01`;
      const monthEnd = `${yyyy}-${mm}-31`;
      const yearStart = `${yyyy}-01-01`;
      const yearEnd = `${yyyy}-12-31`;

      async function sumPaymentsRange(start, end) {
        const snap = await db
          .collection("payments")
          .where("paymentDate", ">=", start)
          .where("paymentDate", "<=", end)
          .get();

        let total = 0;
        let count = 0;
        snap.forEach((doc) => {
          total += Number(doc.data().amount || 0);
          count++;
        });
        return { total, count };
      }

      async function sumRefundsRange(start, end) {
        const snap = await db
          .collection("refunds")
          .where("refundDate", ">=", start)
          .where("refundDate", "<=", end)
          .get();

        let total = 0;
        let count = 0;
        snap.forEach((doc) => {
          total += Number(doc.data().amount || 0);
          count++;
        });
        return { total, count };
      }

      const todayPaymentsSnap = await db
        .collection("payments")
        .where("paymentDate", "==", todayStrISO)
        .get();
      let todayPayTotal = 0;
      let todayPayCount = 0;
      todayPaymentsSnap.forEach((doc) => {
        todayPayTotal += Number(doc.data().amount || 0);
        todayPayCount++;
      });

      const todayRefundsSnap = await db
        .collection("refunds")
        .where("refundDate", "==", todayStrISO)
        .get();
      let todayRefundTotal = 0;
      let todayRefundCount = 0;
      todayRefundsSnap.forEach((doc) => {
        todayRefundTotal += Number(doc.data().amount || 0);
        todayRefundCount++;
      });

      const todayNet = todayPayTotal - todayRefundTotal;

      const monthPayments = await sumPaymentsRange(monthStart, monthEnd);
      const monthRefunds = await sumRefundsRange(monthStart, monthEnd);
      const monthNet = monthPayments.total - monthRefunds.total;

      const yearPayments = await sumPaymentsRange(yearStart, yearEnd);
      const yearRefunds = await sumRefundsRange(yearStart, yearEnd);
      const yearNet = yearPayments.total - yearRefunds.total;

      return {
        today: {
          label: todayLabel,
          paymentsTotal: todayPayTotal,
          paymentsCount: todayPayCount,
          refundsTotal: todayRefundTotal,
          refundsCount: todayRefundCount,
          netTotal: todayNet,
        },
        month: {
          label: `${mm}.${yyyy}`,
          paymentsTotal: monthPayments.total,
          paymentsCount: monthPayments.count,
          refundsTotal: monthRefunds.total,
          refundsCount: monthRefunds.count,
          netTotal: monthNet,
        },
        year: {
          label: `${yyyy}`,
          paymentsTotal: yearPayments.total,
          paymentsCount: yearPayments.count,
          refundsTotal: yearRefunds.total,
          refundsCount: yearRefunds.count,
          netTotal: yearNet,
        },
      };
    });

    res.json(data);
  } catch (err) {
    console.error("dashboard summary error:", err);
    res.status(500).json({ error: "Failed to load dashboard summary" });
  }
});

// ---------- GET /api/bills ----------
app.get("/api/bills", async (_req, res) => {
  try {
    const key = makeCacheKey("bills", "list");

    const data = await getOrSetCache(key, 3, async () => {
      const billsSnap = await db
        .collection("bills")
        .orderBy("invoiceNo", "desc")
        .get();

      const result = [];

      for (const doc of billsSnap.docs) {
        const b = doc.data();
        const billId = doc.id;

        const total = Number(b.total || 0);
        const isProcedureCompleted = b.procedureConfirmed === true;

        // ---------------- PAYMENTS (GROSS) ----------------
        const paysSnap = await db
          .collection("payments")
          .where("billId", "==", billId)
          .get();

        const paidGross = paysSnap.docs.reduce(
          (sum, d) => sum + Number(d.data().amount || 0),
          0
        );

        // ---------------- REFUNDS ----------------
        const refundsSnap = await db
          .collection("refunds")
          .where("billId", "==", billId)
          .get();

        const refunded = refundsSnap.docs.reduce(
          (sum, d) => sum + Number(d.data().amount || 0),
          0
        );

        // ---------------- NET CALCULATION ----------------
        const paidNet = Math.max(paidGross - refunded, 0);

        // 🔥 BUSINESS OVERRIDE
        // Procedure done = balance must be ZERO no matter what
        const balance = isProcedureCompleted
          ? 0
          : Math.max(total - paidNet, 0);

        result.push({
          id: billId,
          invoiceNo: b.invoiceNo || billId,
          patientName: b.patientName || "",
          date: formatDateDot(b.date || null),

          total,
          paid: paidNet,        // NET PAID (2000 / 500 etc)
          refunded,
          balance,

          procedureConfirmed: isProcedureCompleted, // ✅ REQUIRED BY UI
          status: isProcedureCompleted || balance <= 0 ? "PAID" : "PENDING",
        });
      }

      return result;
    });

    res.json(data);
  } catch (err) {
    console.error("GET /api/bills error:", err);
    res.status(500).json({ error: "Failed to fetch bills" });
  }
});


app.post("/api/bills", async (req, res) => {
  try {
    const {
      patientName,
      sex,
      address,
      age,
      date,
      pay,
      paymentMode,
      referenceNo,
      procedureDone,

      // NEW – mode-specific payment fields from CreateBill
      chequeDate,
      chequeNumber,
      bankName,
      transferType,
      transferDate,
      upiName,
      upiId,
      upiDate,

      drawnOn,
      drawnAs,

      // generic remarks
      remarks,

      // service rows from CreateBill (frontend sends description)
      services,
    } = req.body;

    // store date as ISO yyyy-mm-dd for queries, but display as DD.MM.YYYY in outputs
    // const jsDateISO = date || new Date().toISOString().slice(0, 10);
    if (!date) {
      return res.status(400).json({ error: "Bill date is required" });
    }
    const jsDateISO = date; // strictly from frontend

    // 1) SERVICES ko normalize karo
    // Accept frontend { description, qty, rate } and map to { item, details, qty, rate, amount }
    const normalizedServices = Array.isArray(services)
      ? services.map((s) => {
          const qty = Number(s.qty) || 0;
          const rate = Number(s.rate) || 0;
          const amount = qty * rate;
          return {
            item: s.item || s.description || "",
            details: s.details || "",
            qty,
            rate,
            amount,
          };
        })
      : [];

    // 2) ITEMS DATA – items collection + sheet ke liye
    const itemsData = normalizedServices.map((s) => {
      const description = s.item || s.details || "";
      return {
        description,
        qty: s.qty,
        rate: s.rate,
        amount: s.amount,
      };
    });

    // 3) TOTALS (no Subtotal / Adjust)
    const total = itemsData.reduce(
      (sum, it) => sum + Number(it.amount || 0),
      0
    );

    const firstPay = Number(pay) || 0;
    const refunded = 0;
    const effectivePaid = firstPay - refunded;
    const balance = total - effectivePaid;
    const status = computeStatus(total, effectivePaid);

    // 4) Invoice no + billId generate
    const { invoiceNo } = await generateInvoiceNumber(jsDateISO);
    const billId = invoiceNo.replace(/\//g, "_"); // e.g. "25-26_INV-0001"
    const createdAt = new Date().toISOString();

    const billRef = db.collection("bills").doc(billId);
    const batch = db.batch();

    // 5) Bill document (doctor regnos removed)
    batch.set(billRef, {
      patientName: patientName || "",
      sex: sex || null,
      address: address || "",
      age: age ? Number(age) : null,
      date: jsDateISO, // stored ISO for queries; display uses formatDateDot
      procedureDone: procedureDone || null,
      invoiceNo: invoiceNo,
      total,
      paid: firstPay,
      refunded,
      balance,
      status,
      createdAt,
      remarks: remarks || null,
      // store normalized services on the bill for PDFs / future UI
      services: normalizedServices,
    });

    // 6) Items collection (1 doc per item row)
    itemsData.forEach((item, index) => {
      const lineNo = index + 1;
      const itemId = `${billId}-${String(lineNo).padStart(2, "0")}`;

      const qty = Number(item.qty || 0);
      const rate = Number(item.rate || 0);
      const amount = qty * rate;

      const itemRef = db.collection("items").doc(itemId);

      batch.set(itemRef, {
        billId,
        patientName: patientName || "",
        description: item.description || "",
        qty,
        rate,
        amount,
      });
    });

    // 7) Optional first payment
    let paymentDoc = null;
    let receiptDoc = null;

    if (firstPay > 0) {
      const receiptNo = await generateReceiptId(invoiceNo, billId);
      const paymentId = receiptNo.replace(/\//g, "_");
      const paymentRef = db.collection("payments").doc(paymentId);
      const now = new Date();
      const paymentDate = jsDateISO;
      const paymentTime = now.toTimeString().slice(0, 5);
      const paymentDateTime = now.toISOString();

      paymentDoc = {
        billId,
        amount: firstPay,
        mode: paymentMode || "Cash",
        referenceNo: referenceNo || null,
        drawnOn: drawnOn || null,
        drawnAs: drawnAs || null,

        // persist mode-specific extras for first payment too
        chequeDate: chequeDate || null,
        chequeNumber: chequeNumber || null,
        bankName: bankName || null,

        transferType: transferType || null,
        transferDate: transferDate || null,

        upiName: upiName || null,
        upiId: upiId || null,
        upiDate: upiDate || null,

        paymentDate,
        paymentTime,
        paymentDateTime,
        receiptNo,
      };

      batch.set(paymentRef, paymentDoc);
      receiptDoc = { id: paymentId, receiptNo };
    }

    await batch.commit();

    // cache clear
    cache.flushAll();

    // 8) Sheets sync (fire-and-forget)
    syncBillToSheet({
      id: billId,
      invoiceNo: invoiceNo,
      patientName,
      address,
      age: age ? Number(age) : null,
      date: jsDateISO,
      procedureDone,
      total,
      paid: firstPay,
      refunded,
      balance,
      status,
      sex: sex || null,
    });

    syncItemsToSheet(
      billId,
      billId,
      patientName,
      itemsData.map((it) => ({
        description: it.description,
        qty: it.qty,
        rate: it.rate,
        amount: it.amount,
      }))
    );

    // 9) Response (doctorReg removed)
    res.json({
      bill: {
        id: billId,
        invoiceNo: invoiceNo,
        patientName: patientName || "",
        sex: sex || null,
        address: address || "",
        age: age ? Number(age) : null,
        date: formatDateDot(jsDateISO),
        procedureDone: procedureDone || null,
        total,
        paid: firstPay,
        refunded,
        balance,
        status,
        remarks: remarks || null,
        services: normalizedServices,
        paymentMode: paymentDoc?.mode || null,
        referenceNo: paymentDoc?.referenceNo || null,
        drawnOn: paymentDoc?.drawnOn || null,
        drawnAs: paymentDoc?.drawnAs || null,
        chequeDate: paymentDoc?.chequeDate || null,
        chequeNumber: paymentDoc?.chequeNumber || null,
        bankName: paymentDoc?.bankName || null,
        transferType: paymentDoc?.transferType || null,
        transferDate: paymentDoc?.transferDate || null,
        upiName: paymentDoc?.upiName || null,
        upiId: paymentDoc?.upiId || null,
        upiDate: paymentDoc?.upiDate || null,
      },
      payment: paymentDoc,
      receipt: receiptDoc,
    });
  } catch (err) {
    console.error("POST /api/bills error:", err);
    res.status(500).json({ error: "Failed to create bill" });
  }
});


app.get("/api/bills/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const key = makeCacheKey("bill-detail", id);
    const data = await getOrSetCache(key, 3, async () => {
      const billRef = db.collection("bills").doc(id);
      const billSnap = await billRef.get();
      if (!billSnap.exists) {
        throw new Error("NOT_FOUND");
      }

      const bill = billSnap.data();

      // ---------------- ITEMS ----------------
      const itemsSnap = await db
        .collection("items")
        .where("billId", "==", id)
        .get();

      const items = itemsSnap.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));

      // ---------------- PAYMENTS ----------------
      const paysSnap = await db
        .collection("payments")
        .where("billId", "==", id)
        .get();

      let payments = paysSnap.docs.map((doc) => {
        const d = doc.data();
        const paymentDateTime =
          d.paymentDateTime ||
          (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);

        return {
          id: doc.id,
          amount: Number(d.amount || 0),
          mode: d.mode || "",
          referenceNo: d.referenceNo || null,
          receiptNo: d.receiptNo || null,
          date: formatDateDot(d.paymentDate || null),
          time: d.paymentTime || null,
          paymentDateTime,
          drawnOn: d.drawnOn || null,
          drawnAs: d.drawnAs || null,

          chequeDate: d.chequeDate || null,
          chequeNumber: d.chequeNumber || null,
          bankName: d.bankName || null,

          transferType: d.transferType || null,
          transferDate: d.transferDate || null,

          upiName: d.upiName || null,
          upiId: d.upiId || null,
          upiDate: d.upiDate || null,
        };
      });

      payments.sort((a, b) => {
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
        const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
        return da - dbb;
      });

      const totalPaidGross = payments.reduce(
        (sum, p) => sum + Number(p.amount || 0),
        0
      );

      // ---------------- REFUNDS ----------------
      const refundsSnap = await db
        .collection("refunds")
        .where("billId", "==", id)
        .get();

      let refunds = refundsSnap.docs.map((doc) => {
        const d = doc.data();
        const refundDateTime =
          d.refundDateTime ||
          (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);

        return {
          id: doc.id,
          amount: Number(d.amount || 0),
          mode: d.mode || "",
          referenceNo: d.referenceNo || null,
          refundNo: d.refundReceiptNo || null,
          date: formatDateDot(d.refundDate || null),
          time: d.refundTime || null,
          refundDateTime,
          drawnOn: d.drawnOn || null,
          drawnAs: d.drawnAs || null,

          chequeDate: d.chequeDate || null,
          chequeNumber: d.chequeNumber || null,
          bankName: d.bankName || null,

          transferType: d.transferType || null,
          transferDate: d.transferDate || null,

          upiName: d.upiName || null,
          upiId: d.upiId || null,
          upiDate: d.upiDate || null,
        };
      });

      refunds.sort((a, b) => {
        const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
        const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
        return da - dbb;
      });

      const totalRefunded = refunds.reduce(
        (sum, r) => sum + Number(r.amount || 0),
        0
      );

      // ---------------- FINAL CALCULATION ----------------
      const total = Number(bill.total || 0);
      const netPaid = totalPaidGross - totalRefunded;
      const balance = total - netPaid;
      const status = computeStatus(total, netPaid);

      const primaryPayment = payments[0] || null;

      // ---------------- RESPONSE ----------------
      return {
        id,
        invoiceNo: bill.invoiceNo || id,
        patientName: bill.patientName || "",
        sex: bill.sex || null,
        address: bill.address || "",
        age: bill.age || null,
        date: bill.date || null,
        procedureDone: bill.procedureDone || null,
        procedureConfirmed: bill.procedureConfirmed || false, // ✅ ADD THIS LINE

        total,
        paid: netPaid,                 // ✅ NET PAID
        totalPaid: totalPaidGross,     // ✅ GROSS PAID
        refunded: totalRefunded,
        balance,
        status,

        remarks: bill.remarks || null,
        services: bill.services || null,

        items,
        payments,
        refunds,

        paymentMode: primaryPayment?.mode || null,
        referenceNo: primaryPayment?.referenceNo || null,
        drawnOn: primaryPayment?.drawnOn || null,
        drawnAs: primaryPayment?.drawnAs || null,
      };
    });

    res.json(data);
  } catch (err) {
    if (err.message === "NOT_FOUND") {
      return res.status(404).json({ error: "Bill not found" });
    }
    console.error("bill detail error:", err);
    res.status(500).json({ error: "Failed to load bill" });
  }
});


// app.patch("/api/bills/:id", async (req, res) => {
//   const id = req.params.id;
//   const { procedureConfirmed } = req.body;

//   if (!id) {
//     return res.status(400).json({ error: "Invalid bill id" });
//   }

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();

//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     // Update the bill with procedureConfirmed flag
//     await billRef.update({
//       procedureConfirmed: procedureConfirmed === true,
//       updatedAt: new Date().toISOString()
//     });

//     res.json({ 
//       success: true, 
//       message: "Bill updated successfully",
//       procedureConfirmed 
//     });
//   } catch (err) {
//     console.error("Error updating bill:", err);
//     res.status(500).json({ error: "Failed to update bill" });
//   }
// });


// ---------- POST /api/bills/:id/payments (add partial payment) ----------

// app.patch("/api/bills/:id", async (req, res) => {
//   const id = req.params.id;
//   const { procedureConfirmed } = req.body;

//   if (!id) {
//     return res.status(400).json({ error: "Invalid bill id" });
//   }

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();

//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     // ✅ Update bill
//     await billRef.update({
//       procedureConfirmed: procedureConfirmed === true,
//       updatedAt: new Date().toISOString(),
//     });

//     // 🔥 CRITICAL FIX — CLEAR STALE CACHE
//     deleteCache(makeCacheKey("bill-detail", id));
//     deleteCache(makeCacheKey("bills", "list"));

//     res.json({
//       success: true,
//       procedureConfirmed: true,
//       message: "Procedure marked as completed",
//     });
//   } catch (err) {
//     console.error("PATCH /api/bills/:id error:", err);
//     res.status(500).json({ error: "Failed to update bill" });
//   }
// });


app.patch("/api/bills/:id", async (req, res) => {
  const id = req.params.id;
  const { procedureConfirmed } = req.body;

  try {
    const billRef = db.collection("bills").doc(id);
    const snap = await billRef.get();

    if (!snap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    await billRef.update({
      procedureConfirmed: procedureConfirmed === true,
      updatedAt: new Date().toISOString(),
    });

    // safe cache clear
    // try {
    //   if (typeof deleteCache === "function") {
    //     deleteCache(makeCacheKey("bill-detail", id));
    //     deleteCache(makeCacheKey("bills", "list"));
    //   }
    // } catch (e) {
    //   console.warn("Cache clear skipped:", e.message);
    // }

    res.json({ success: true, procedureConfirmed: true });
  } catch (err) {
    console.error("PATCH bill error:", err);
    res.status(500).json({ error: err.message });
  }
});



app.post("/api/bills/:id/payments", async (req, res) => {
  const billId = req.params.id;
  if (!billId) {
    return res.status(400).json({ error: "Invalid bill id" });
  }

  const {
    amount,
    mode,
    referenceNo,
    drawnOn,
    drawnAs,

    // 🔵 ADD THIS - accept paymentDate from frontend
    date: requestedPaymentDate,

    // mode-specific fields
    chequeDate,
    chequeNumber,
    bankName,
    transferType,
    transferDate,
    upiName,
    upiId,
    upiDate,
  } = req.body;

  const numericAmount = Number(amount);

  if (!numericAmount || numericAmount <= 0) {
    return res.status(400).json({ error: "Amount must be > 0" });
  }

  try {
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();

    const now = new Date();

    // 🔵 MODIFIED - use requested date if provided, else use today
    const paymentDate = requestedPaymentDate || now.toISOString().slice(0, 10);

    const paymentTime = now.toTimeString().slice(0, 5);
    const paymentDateTime = now.toISOString();

    const invoiceNo = bill.invoiceNo || billId;
    const receiptNo = await generateReceiptId(invoiceNo, billId);
    const paymentId = receiptNo.replace(/\//g, "_");
    const paymentRef = db.collection("payments").doc(paymentId);

    const paymentDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,

      chequeDate: chequeDate || null,
      chequeNumber: chequeNumber || null,
      bankName: bankName || null,

      transferType: transferType || null,
      transferDate: transferDate || null,

      upiName: upiName || null,
      upiId: upiId || null,
      upiDate: upiDate || null,

      paymentDate, // 🔵 Now uses requested date
      paymentTime,
      paymentDateTime,
      receiptNo,
    };

    const oldPaid = Number(bill.paid || 0);
    const oldRefunded = Number(bill.refunded || 0);
    const newPaid = oldPaid + numericAmount;
    const effectivePaid = newPaid - oldRefunded;
    const total = Number(bill.total || 0);
    const newBalance = total - effectivePaid;
    const newStatus = computeStatus(total, effectivePaid);

    const batch = db.batch();
    batch.set(paymentRef, paymentDoc);
    batch.update(billRef, {
      paid: newPaid,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();

    cache.flushAll();

    syncPaymentToSheet(
      { id: paymentRef.id, ...paymentDoc },
      { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
    );

    res.status(201).json({
      id: paymentId,
      ...paymentDoc,
      paymentDateFormatted: formatDateDot(paymentDate),
    });
  } catch (err) {
    console.error("payment error:", err);
    res.status(500).json({ error: "Payment failed" });
  }
});

// POST /api/bills/:id/refunds (Line ~758)
app.post("/api/bills/:id/refunds", async (req, res) => {
  const billId = req.params.id;
  if (!billId) {
    return res.status(400).json({ error: "Invalid bill id" });
  }

  const {
    amount,
    mode,
    referenceNo,
    drawnOn,
    drawnAs,
    date: requestedRefundDate,
    chequeDate,
    chequeNumber,
    bankName,
    transferType,
    transferDate,
    upiName,
    upiId,
    upiDate,
  } = req.body;

  const numericAmount = Number(amount);

  if (!numericAmount || numericAmount <= 0) {
    return res.status(400).json({ error: "Amount must be > 0" });
  }

  try {
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();
    const total = Number(bill.total || 0);
    
    // ✅ FIX: bill.paid is ALREADY net paid amount
    // It gets updated after each refund: paid = paidGross - totalRefunded
    const currentNetPaid = Number(bill.paid || 0);

    // ✅ FIX: Check against current net paid (not gross - refunded again)
    if (numericAmount > currentNetPaid) {
      return res.status(400).json({
        error: "Cannot refund more than net paid amount",
        details: {
          currentNetPaid: currentNetPaid,
          requestedRefund: numericAmount,
          maxRefundable: currentNetPaid
        }
      });
    }

    const now = new Date();
    const refundDate = requestedRefundDate || now.toISOString().slice(0, 10);
    const refundTime = now.toTimeString().slice(0, 5);
    const refundDateTime = now.toISOString();
    const invoiceNo = bill.invoiceNo || billId;
    const refundReceiptNo = await generateRefundId(invoiceNo, billId);
    const refundId = refundReceiptNo.replace(/\//g, "_");
    const refundRef = db.collection("refunds").doc(refundId);

    const refundDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,

      chequeDate: chequeDate || null,
      chequeNumber: chequeNumber || null,
      bankName: bankName || null,

      transferType: transferType || null,
      transferDate: transferDate || null,

      upiName: upiName || null,
      upiId: upiId || null,
      upiDate: upiDate || null,

      refundDate,
      refundTime,
      refundDateTime,
      refundReceiptNo,
    };

    // ✅ Update bill.paid (net paid after refund)
    const newNetPaid = currentNetPaid - numericAmount;
    const newBalance = total - newNetPaid;
    const newStatus = computeStatus(total, newNetPaid);
    
    // ✅ Update bill.refunded (cumulative refunds)
    const oldRefunded = Number(bill.refunded || 0);
    const newRefunded = oldRefunded + numericAmount;

    const batch = db.batch();
    batch.set(refundRef, refundDoc);
    batch.update(billRef, {
      paid: newNetPaid,
      refunded: newRefunded,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();

    cache.flushAll();

    syncRefundToSheet(
      {
        id: refundRef.id,
        ...refundDoc,
        netPaidAfterThis: newNetPaid,
        balanceAfterThis: newBalance,
      },
      { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
    );

    res.status(201).json({
      id: refundId,
      ...refundDoc,
      refundDateFormatted: formatDateDot(refundDate),
    });
  } catch (err) {
    console.error("refund error:", err);
    res.status(500).json({ error: "Refund failed" });
  }
});

// ---------- GET /api/payments/:id (FOR EDIT PAYMENT) ----------
app.get("/api/payments/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    const ref = db.collection("payments").doc(id);
    const snap = await ref.get();

    if (!snap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }

    const p = snap.data();

    res.json({
      id,
      billId: p.billId,
      amount: Number(p.amount || 0),
      mode: p.mode || "Cash",

      // 🔴 IMPORTANT: date must be yyyy-mm-dd for <input type="date">
      paymentDate: p.paymentDate || "",

      referenceNo: p.referenceNo || "",
      drawnOn: p.drawnOn || "",
      drawnAs: p.drawnAs || "",

      chequeDate: p.chequeDate || "",
      chequeNumber: p.chequeNumber || "",
      bankName: p.bankName || "",

      transferType: p.transferType || "",
      transferDate: p.transferDate || "",

      upiName: p.upiName || "",
      upiId: p.upiId || "",
      upiDate: p.upiDate || "",
    });
  } catch (err) {
    console.error("GET /api/payments/:id error:", err);
    res.status(500).json({ error: "Failed to load payment" });
  }
});


app.put("/api/payments/:id", async (req, res) => {
  const paymentId = req.params.id;
  if (!paymentId) return res.status(400).json({ error: "Invalid payment id" });

  const {
    amount,
    mode,
    referenceNo,
    drawnOn,
    drawnAs,

    // 🔵 ADDED - accept paymentDate for editing
    paymentDate,

    chequeDate,
    chequeNumber,
    bankName,
    transferType,
    transferDate,
    upiName,
    upiId,
    upiDate,
  } = req.body;

  const newAmount = Number(amount);
  if (Number.isNaN(newAmount) || newAmount <= 0) {
    return res.status(400).json({ error: "Amount must be > 0" });
  }

  try {
    const paymentRef = db.collection("payments").doc(paymentId);
    const paymentSnap = await paymentRef.get();
    if (!paymentSnap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }

    const oldPayment = paymentSnap.data();
    const billId = oldPayment.billId;

    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();

    // ---- recompute totals ----
    const delta = newAmount - Number(oldPayment.amount || 0);
    const oldPaid = Number(bill.paid || 0);
    const refunded = Number(bill.refunded || 0);

    const newPaid = oldPaid + delta;
    const effectivePaid = newPaid - refunded;
    const total = Number(bill.total || 0);
    const newBalance = total - effectivePaid;
    const newStatus = computeStatus(total, effectivePaid);

    const finalMode = mode || oldPayment.mode;

    const updatedPayment = cleanFieldsByMode(finalMode, {
      amount: newAmount,
      mode: finalMode,
      referenceNo: referenceNo ?? null,
      drawnOn: drawnOn ?? null,
      drawnAs: drawnAs ?? null,

      // 🔵 ADDED - update paymentDate if provided
      paymentDate: paymentDate || oldPayment.paymentDate,

      chequeDate: chequeDate ?? null,
      chequeNumber: chequeNumber ?? null,
      bankName: bankName ?? null,

      transferType: transferType ?? null,
      transferDate: transferDate ?? null,

      upiName: upiName ?? null,
      upiId: upiId ?? null,
      upiDate: upiDate ?? null,
    });

    const batch = db.batch();

    batch.update(paymentRef, updatedPayment);

    batch.update(billRef, {
      paid: newPaid,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();
    cache.flushAll();

    res.json({
      success: true,
      paymentId,
      amount: newAmount,
      billId,
      paid: newPaid,
      balance: newBalance,
      status: newStatus,
    });
  } catch (err) {
    console.error("EDIT payment error:", err);
    res.status(500).json({ error: "Failed to edit payment" });
  }
});

// ---------- PUT /api/refunds/:id (EDIT REFUND) ----------
app.put("/api/refunds/:id", async (req, res) => {
  const refundId = req.params.id;
  if (!refundId) return res.status(400).json({ error: "Invalid refund id" });

  const {
    amount,
    mode,
    refundDate,
    referenceNo,
    drawnOn,
    drawnAs,

    chequeDate,
    chequeNumber,
    bankName,

    transferType,
    transferDate,

    upiName,
    upiId,
    upiDate,
  } = req.body;

  const newAmount = Number(amount);
  if (!newAmount || newAmount <= 0) {
    return res.status(400).json({ error: "Refund amount must be > 0" });
  }

  try {
    const refundRef = db.collection("refunds").doc(refundId);
    const refundSnap = await refundRef.get();

    if (!refundSnap.exists) {
      return res.status(404).json({ error: "Refund not found" });
    }

    const oldRefund = refundSnap.data();
    const billId = oldRefund.billId;

    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();

    // 🔴 CORE LOGIC — adjust bill totals safely
    const oldRefundAmount = Number(oldRefund.amount || 0);
    const delta = newAmount - oldRefundAmount;

    const oldRefunded = Number(bill.refunded || 0);
    const oldNetPaid = Number(bill.paid || 0);
    const total = Number(bill.total || 0);

    const newRefunded = oldRefunded + delta;
    const newNetPaid = oldNetPaid - delta;
    const newBalance = total - newNetPaid;
    const newStatus = computeStatus(total, newNetPaid);

    if (newNetPaid < 0) {
      return res.status(400).json({
        error: "Refund exceeds paid amount",
      });
    }

    const batch = db.batch();

    batch.update(refundRef, {
      amount: newAmount,
      mode: mode || "Cash",
      refundDate,
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,

      chequeDate: chequeDate || null,
      chequeNumber: chequeNumber || null,
      bankName: bankName || null,

      transferType: transferType || null,
      transferDate: transferDate || null,

      upiName: upiName || null,
      upiId: upiId || null,
      upiDate: upiDate || null,
    });

    batch.update(billRef, {
      refunded: newRefunded,
      paid: newNetPaid,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();

    cache.flushAll();

    res.json({
      success: true,
      refundId,
      newNetPaid,
      newBalance,
      newRefunded,
    });
  } catch (err) {
    console.error("PUT /api/refunds/:id error:", err);
    res.status(500).json({ error: "Failed to update refund" });
  }
});

app.put("/api/bills/:id", async (req, res) => {
  const billId = req.params.id;
  if (!billId) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const oldBill = billSnap.data();

    // Editable fields from frontend (everything except payment info)
    const {
      patientName,
      sex,
      address,
      age,
      date,
      remarks,
      services,
      procedureDone, // ✅ ADD
    } = req.body;

    if (!date && !oldBill.date) {
      return res.status(400).json({ error: "Bill date is required" });
    }
    const jsDate = date || oldBill.date;

    // --- NORMALIZE SERVICES (same style as POST /api/bills) ---
    const normalizedServices = Array.isArray(services)
      ? services.map((s) => {
          const qty = Number(s.qty) || 0;
          const rate = Number(s.rate) || 0;
          const amount = qty * rate;
          return {
            item: s.item || s.description || "",
            details: s.details || "",
            qty,
            rate,
            amount,
          };
        })
      : [];

    // YE data hum ITEMS collection me likhenge
    const itemsData = normalizedServices.map((s) => {
      const description = s.item || s.details || "";
      return {
        description,
        qty: s.qty,
        rate: s.rate,
        amount: s.amount,
      };
    });

    const total = itemsData.reduce(
      (sum, it) => sum + Number(it.amount || 0),
      0
    );

    // keep payments/refunds as is
    const paidGross = Number(oldBill.paid || 0);
    const refunded = Number(oldBill.refunded || 0);
    const effectivePaid = paidGross - refunded;
    const balance = total - effectivePaid;
    const status = computeStatus(total, effectivePaid);

    const batch = db.batch();

    // 1) Update bill doc
    const finalPatientName = patientName ?? oldBill.patientName ?? "";

    batch.update(billRef, {
      patientName: finalPatientName,
      sex: sex ?? oldBill.sex ?? null,
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      procedureDone:
        typeof procedureDone !== "undefined"
          ? procedureDone
          : oldBill.procedureDone ?? null,

      total,
      paid: paidGross,
      refunded,
      balance,
      status,
      remarks:
        typeof remarks !== "undefined" ? remarks : oldBill.remarks ?? null,
      services: normalizedServices,
    });

    // 2) Replace items collection for this bill
    //    (pehle saare old items delete, fir naya set create)
    const existingItemsSnap = await db
      .collection("items")
      .where("billId", "==", billId)
      .get();

    existingItemsSnap.forEach((doc) => {
      batch.delete(doc.ref);
    });

    // 3) NEW ITEMS INSERT with deterministic ID:
    itemsData.forEach((item, index) => {
      const lineNo = index + 1;
      const itemId = `${billId}-${String(lineNo).padStart(2, "0")}`; // e.g. 25-26_INV-0001-01

      const qty = Number(item.qty || 0);
      const rate = Number(item.rate || 0);
      const amount = Number(item.amount || qty * rate || 0);

      batch.set(db.collection("items").doc(itemId), {
        billId,
        patientName: finalPatientName,
        description: item.description,
        qty,
        rate,
        amount,
      });
    });

    await batch.commit();

    // clear caches so GET /api/bills and /api/bills/:id show updated data
    cache.flushAll();

    // update Google Sheet (optional but consistent with create)
    syncBillToSheet({
      id: billId,
      invoiceNo: oldBill.invoiceNo || billId,
      patientName: finalPatientName,
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      total,
      paid: paidGross,
      refunded,
      balance,
      status,
      sex: sex ?? oldBill.sex ?? null,
    });

    syncItemsToSheet(
      billId,
      billId,
      finalPatientName,
      itemsData.map((it) => ({
        description: it.description,
        qty: it.qty,
        rate: it.rate,
        amount: it.amount,
      }))
    );

    res.json({
      id: billId,
      invoiceNo: oldBill.invoiceNo || billId,
      patientName: finalPatientName,
      sex: sex ?? oldBill.sex ?? null,
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      total,
      paid: paidGross,
      refunded,
      balance,
      status,
      remarks:
        typeof remarks !== "undefined" ? remarks : oldBill.remarks ?? null,
      services: normalizedServices,
    });
  } catch (err) {
    console.error("PUT /api/bills/:id error:", err);
    res.status(500).json({ error: "Failed to update bill" });
  }
});


// ---------- GET /api/refunds/:id (FOR EDIT REFUND) ----------
app.get("/api/refunds/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid refund id" });

  try {
    const ref = db.collection("refunds").doc(id);
    const snap = await ref.get();

    if (!snap.exists) {
      return res.status(404).json({ error: "Refund not found" });
    }

    const r = snap.data();

    res.json({
      id,
      billId: r.billId,
      amount: Number(r.amount || 0),
      mode: r.mode || "Cash",

      // 🔴 MUST be yyyy-mm-dd for input[type=date]
      refundDate: r.refundDate || "",

      referenceNo: r.referenceNo || "",
      drawnOn: r.drawnOn || "",
      drawnAs: r.drawnAs || "",

      chequeDate: r.chequeDate || "",
      chequeNumber: r.chequeNumber || "",
      bankName: r.bankName || "",

      transferType: r.transferType || "",
      transferDate: r.transferDate || "",

      upiName: r.upiName || "",
      upiId: r.upiId || "",
      upiDate: r.upiDate || "",
    });
  } catch (err) {
    console.error("GET /api/refunds/:id error:", err);
    res.status(500).json({ error: "Failed to load refund" });
  }
});


// ---------- PDF: Payment Receipt (A4 half page, professional layout) ----------
app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    const paymentRef = db.collection("payments").doc(id);
    const paymentSnap = await paymentRef.get();
    if (!paymentSnap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }
    const payment = paymentSnap.data();
    const billId = payment.billId;

    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();
    const billTotal = Number(bill.total || 0);

    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();

    const allPayments = paysSnap.docs
      .map((doc) => {
        const d = doc.data();
        const paymentDateTime =
          d.paymentDateTime ||
          (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
        return {
          id: doc.id,
          paymentDateTime,
          amount: Number(d.amount || 0),
        };
      })
      .sort((a, b) => {
        const da = a.paymentDateTime
          ? new Date(a.paymentDateTime)
          : new Date(0);
        const dbb = b.paymentDateTime
          ? new Date(b.paymentDateTime)
          : new Date(0);
        return da - dbb;
      });

    let cumulativePaid = 0;
    let paidTillThis = 0;
    let balanceAfterThis = billTotal;

    for (const p of allPayments) {
      cumulativePaid += p.amount;
      if (p.id === id) {
        paidTillThis = cumulativePaid;
        balanceAfterThis = billTotal - paidTillThis;
        break;
      }
    }

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const patientName = bill.patientName || "";
    const address = bill.address || "";
    const drawnOn = payment.drawnOn || null;
    const drawnAs = payment.drawnAs || null;
    const mode = payment.mode || "Cash";
    const referenceNo = payment.referenceNo || null;
    const receiptNo = payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

    const chequeDate = payment.chequeDate || null;
    const chequeNumber = payment.chequeNumber || null;
    const bankName = payment.bankName || null;
    const transferType = payment.transferType || null;
    const transferDate = payment.transferDate || null;
    const upiName = payment.upiName || null;
    const upiId = payment.upiId || null;
    const upiDate = payment.upiDate || null;

    // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
    const profile = await getClinicProfile({ force: true });
    const clinicName = profileValue(profile, "clinicName");
    const clinicAddress = profileValue(profile, "address");
    const clinicPAN = profileValue(profile, "pan");
    const clinicRegNo = profileValue(profile, "regNo");
    const doctor1Name = profileValue(profile, "doctor1Name");
    const doctor1RegNo = profileValue(profile, "doctor1RegNo");
    const doctor2Name = profileValue(profile, "doctor2Name");
    const doctor2RegNo = profileValue(profile, "doctor2RegNo");
    const patientRepresentative = profileValue(
      profile,
      "patientRepresentative"
    );
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="receipt-${id}.pdf"`
    );

    const doc = new PDFDocument({
      size: "A4",
      margin: 36,
    });

    doc.pipe(res);

    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72;
    let y = 40;

    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

    const leftLogoX = 36;
    const rightLogoX = pageWidth - 36;

    // HEADER
    try {
      doc.image(logoLeftPath, leftLogoX, y, { width: 32, height: 32 });
    } catch (e) {}
    try {
      doc.image(logoRightPath, rightLogoX - 32, y, { width: 32, height: 32 });
    } catch (e) {}

    doc
      .font("Helvetica-Bold")
      .fontSize(13)
      .text(clinicName || "", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(clinicAddress || "", 0, y + 20, {
        align: "center",
        width: pageWidth,
      })
      .text(
        (clinicPAN ? `PAN: ${clinicPAN}` : "") +
          (clinicPAN && clinicRegNo ? "   |   " : "") +
          (clinicRegNo ? `Reg. No.: ${clinicRegNo}` : ""),
        {
          align: "center",
          width: pageWidth,
        }
      );

    y += 48;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;

    // DOCTOR LINE (from profile)
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text(doctor1Name || "", 36, y);
    doc.text(doctor2Name || "", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
    doc.text(
      doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );

    y += 16;

    // TITLE BAR
    doc
      .save()
      .rect(36, y, usableWidth, 18)
      .fill("#F3F3F3")
      .restore()
      .rect(36, y, usableWidth, 18)
      .stroke();

    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("PAYMENT RECEIPT", 36, y + 4, {
        align: "center",
        width: usableWidth,
      });

    y += 26;

    // COMMON LAYOUT (left details + right summary)
    const leftX = 36;
    const rightBoxWidth = 180;
    const rightX = pageWidth - 36 - rightBoxWidth;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Receipt No.: ${receiptNo}`, leftX, y);
    doc.text(`Date: ${formatDateDot(payment.paymentDate || "")}`, rightX, y, {
      width: rightBoxWidth,
      align: "right",
    });

    y += 16;

    const detailsTopY = y;
    const leftWidth = rightX - leftX - 10;

    // doc
    //   .font("Helvetica-Bold")
    //   .text(`Patient Name: ${patientName}`, leftX, detailsTopY, {
    //     width: leftWidth,
    //   });

    // let detailsY = doc.y + 4;
    doc
      .font("Helvetica-Bold")
      .text(`Patient Name: ${patientName}`, leftX, detailsTopY, {
        width: leftWidth,
      });

    doc.font("Helvetica").text(`Address: ${address}`, leftX, doc.y + 3, {
      width: leftWidth,
    });

    let detailsY = doc.y + 4;

    doc.font("Helvetica");

    const addDetail = (label, value) => {
      if (!value) return;
      doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
      detailsY = doc.y + 3;
    };

    addDetail("Amount Received: Rs", formatMoney(payment.amount));
    addDetail("Payment Mode:", mode);
    addDetail("Cheque No.:", chequeNumber);
    addDetail("Cheque Date:", formatDateDot(chequeDate));
    addDetail("Bank:", bankName);
    addDetail("Transfer Type:", transferType);
    addDetail("Transfer Date:", formatDateDot(transferDate));
    addDetail("UPI ID:", upiId);
    addDetail("UPI Name:", upiName);
    addDetail("UPI Date:", formatDateDot(upiDate));
    addDetail("UTR NO./REF NO.:", referenceNo);
    addDetail("Drawn On:", drawnOn);
    addDetail("Drawn As:", drawnAs);

    // RIGHT BILL SUMMARY BOX
    const boxY = detailsTopY;
    const lineH = 12;
    const boxHeight = 100;

    doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

    let by = boxY + 4;
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .text("Bill Summary", rightX + 6, by);
    by += lineH + 2;
    doc.font("Helvetica").fontSize(9);

    const billNoText = bill.invoiceNo || billId;

    const addRow = (label, value) => {
      doc.text(label, rightX + 6, by);
      doc.text(value, rightX + 6, by, {
        width: rightBoxWidth - 12,
        align: "right",
      });
      by += lineH;
    };

    addRow("Bill No.:", billNoText);
    addRow("Bill Date:", formatDateDot(bill.date || ""));
    addRow("Bill Total:", `Rs ${formatMoney(billTotal)}`);
    addRow("Paid Amount:", `Rs ${formatMoney(paidTillThis)}`);
    addRow("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

    // FOOTNOTE + SIGNATURES
    y = Math.max(detailsY + 6, boxY + boxHeight + 6);

    doc
      .font("Helvetica")
      .fontSize(8)
      .text("* Dispute if any subject to Jamshedpur Jurisdiction", leftX, y, {
        width: usableWidth,
      });

    const sigY = y + 40;
    const sigWidth = 160;

    // doc
    //   .moveTo(leftX, sigY)
    //   .lineTo(leftX + sigWidth, sigY)
    //   .dash(1, { space: 2 })
    //   .stroke()
    //   .undash();
    // doc.fontSize(8).text(patientRepresentative || "", leftX, sigY + 4, {
    //   width: sigWidth,
    //   align: "center",
    // });

    const rightSigX = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    doc.end();
  } catch (err) {
    console.error("receipt-html-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate receipt PDF" });
    }
  }
});


// app.get("/api/refunds/:id/refund-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid refund id" });

//   try {
//     // ---------- REFUND ----------
//     const refundRef = db.collection("refunds").doc(id);
//     const refundSnap = await refundRef.get();
//     if (!refundSnap.exists) {
//       return res.status(404).json({ error: "Refund not found" });
//     }
//     const refund = refundSnap.data();
//     const billId = refund.billId;

//     // ---------- BILL ----------
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();
//     const billTotal = Number(bill.total || 0);

//     // ---------- PAYMENTS (GROSS) ----------
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       return {
//         amount: Number(d.amount || 0),
//         date: new Date(
//           d.paymentDateTime ||
//             (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : 0)
//         ),
//       };
//     });

//     const totalPaidGross = payments.reduce((s, p) => s + p.amount, 0);

//     // ---------- REFUNDS (ORDERED) ----------
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const refunds = refundsSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         return {
//           id: doc.id,
//           amount: Number(d.amount || 0),
//           date: new Date(
//             d.refundDateTime ||
//               (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : 0)
//           ),
//         };
//       })
//       .sort((a, b) => a.date - b.date);

//     // ---------- REFUND TILL THIS RECEIPT ----------
//     let refundedTillThis = 0;
//     for (const r of refunds) {
//       refundedTillThis += r.amount;
//       if (r.id === id) break;
//     }

//     // ---------- FINAL NUMBERS (FIXED) ----------
//     // Net amount that customer has actually paid (after deducting refunds)
//     const netPaidTillThis = totalPaidGross - refundedTillThis;
    
//     // Balance = Bill Total - Net Paid
//     // If Net Paid < Bill Total, customer owes money (positive balance)
//     // If Net Paid >= Bill Total, balance should be 0
//     const balanceAfterThis = Math.max(0, billTotal - netPaidTillThis);

//     // ---------- HELPERS ----------
//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // Assuming you already have these utility functions somewhere
//     // formatDateDot, getClinicProfile, profileValue, etc.

//     const patientName = bill.patientName || "";
//     const address = bill.address || "";
//     const drawnOn = refund.drawnOn || null;
//     const drawnAs = refund.drawnAs || null;
//     const mode = refund.mode || "Cash";
//     const referenceNo = refund.referenceNo || null;
//     const refundNo =
//       refund.refundReceiptNo || `F-${String(id).padStart(4, "0")}`;

//     const chequeDate = refund.chequeDate || null;
//     const chequeNumber = refund.chequeNumber || null;
//     const bankName = refund.bankName || null;
//     const transferType = refund.transferType || null;
//     const transferDate = refund.transferDate || null;
//     const upiName = refund.upiName || null;
//     const upiId = refund.upiId || null;
//     const upiDate = refund.upiDate || null;

//     // ---------- CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // ---------- PDF HEADERS ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="refund-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 40;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftLogoX = 36;
//     const rightLogoX = pageWidth - 36;

//     // HEADER
//     try {
//       doc.image(logoLeftPath, leftLogoX, y, { width: 32, height: 32 });
//     } catch (e) {}
//     try {
//       doc.image(logoRightPath, rightLogoX - 32, y, { width: 32, height: 32 });
//     } catch (e) {}

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(13)
//       .text(clinicName || "", 0, y + 2, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 20, {
//         align: "center",
//         width: pageWidth,
//       })
//       .text(
//         (clinicPAN ? `PAN: ${clinicPAN}` : "") +
//           (clinicPAN && clinicRegNo ? "   |   " : "") +
//           (clinicRegNo ? `Reg. No.: ${clinicRegNo}` : ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 6;

//     // DOCTOR LINE
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(
//       doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
//       pageWidth / 2,
//       y,
//       {
//         align: "right",
//         width: usableWidth / 2,
//       }
//     );

//     y += 16;

//     // TITLE BAR
//     doc
//       .save()
//       .rect(36, y, usableWidth, 18)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 18)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(10).text("REFUND RECEIPT", 36, y + 4, {
//       align: "center",
//       width: usableWidth,
//     });

//     y += 26;

//     // COMMON LAYOUT
//     const leftX = 36;
//     const rightBoxWidth = 180;
//     const rightX = pageWidth - 36 - rightBoxWidth;

//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Refund No.: ${refundNo}`, leftX, y);
//     doc.text(`Date: ${formatDateDot(refund.refundDate || "")}`, rightX, y, {
//       width: rightBoxWidth,
//       align: "right",
//     });

//     y += 16;

//     const detailsTopY = y;
//     const leftWidth = rightX - leftX - 10;

//     doc
//       .font("Helvetica-Bold")
//       .text(`Patient Name: ${patientName}`, leftX, detailsTopY, {
//         width: leftWidth,
//       });
//     doc.font("Helvetica").text(`Address: ${address}`, leftX, doc.y + 3, {
//       width: leftWidth,
//     });

//     let detailsY = doc.y + 4;
//     doc.font("Helvetica");

//     const addDetailR = (label, value) => {
//       if (!value) return;
//       doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
//       detailsY = doc.y + 3;
//     };

//     addDetailR("Amount Refunded: Rs", formatMoney(refund.amount));
//     addDetailR("Refund Mode:", mode);
//     addDetailR("Reference No.:", referenceNo);
//     addDetailR("Drawn On:", drawnOn);
//     addDetailR("Drawn As:", drawnAs);
//     addDetailR("Cheque No.:", chequeNumber);
//     addDetailR("Cheque Date:", formatDateDot(chequeDate));
//     addDetailR("Bank:", bankName);
//     addDetailR("Transfer Type:", transferType);
//     addDetailR("Transfer Date:", formatDateDot(transferDate));
//     addDetailR("UPI ID:", upiId);
//     addDetailR("UPI Name:", upiName);
//     addDetailR("UPI Date:", formatDateDot(upiDate));

//     // RIGHT BILL SUMMARY
//     const boxY = detailsTopY;
//     const lineH = 12;
//     const boxHeight = 100;

//     doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

//     let by2 = boxY + 4;
//     doc.font("Helvetica-Bold").fontSize(9).text("Bill Summary", rightX + 6, by2);
//     by2 += lineH + 2;
//     doc.font("Helvetica").fontSize(9);

//     const billNoText2 = bill.invoiceNo || billId;

//     const addRow2 = (label, value) => {
//       doc.text(label, rightX + 6, by2);
//       doc.text(value, rightX + 6, by2, {
//         width: rightBoxWidth - 12,
//         align: "right",
//       });
//       by2 += lineH;
//     };

//     addRow2("Bill No.:", billNoText2);
//     addRow2("Bill Date:", formatDateDot(bill.date || ""));
//     addRow2("Bill Total:", `Rs ${formatMoney(billTotal)}`);
//     addRow2("Total Paid:", `Rs ${formatMoney(netPaidTillThis)}`);
//     //addRow2("Refunded (incl. this):", `Rs ${formatMoney(refundedTillThis)}`);
//     addRow2("Refunded (incl. this):", `Rs ${formatMoney(refund.amount)}`);
//     addRow2("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

//     // FOOTNOTE + SIGNATURES
//     y = Math.max(detailsY + 6, boxY + boxHeight + 6);

//     doc
//       .font("Helvetica")
//       .fontSize(8)
//       .text("* Dispute if any subject to Jamshedpur Jurisdiction", leftX, y, {
//         width: usableWidth,
//       });

//     const sigY = y + 24;
//     const sigWidth = 160;

//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     doc.end();
//   } catch (err) {
//     console.error("refund-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate refund PDF" });
//     }
//   }
// });


// app.get("/api/refunds/:id/refund-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid refund id" });

//   try {
//     // ---------- REFUND ----------
//     const refundRef = db.collection("refunds").doc(id);
//     const refundSnap = await refundRef.get();
//     if (!refundSnap.exists) {
//       return res.status(404).json({ error: "Refund not found" });
//     }
//     const refund = refundSnap.data();
//     const billId = refund.billId;

//     // ---------- BILL ----------
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();
//     const billTotal = Number(bill.total || 0);
//     const isProcedureCompleted = bill.procedureConfirmed === true; // ✅ ADD THIS

//     // ---------- PAYMENTS (GROSS) ----------
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       return {
//         amount: Number(d.amount || 0),
//         date: new Date(
//           d.paymentDateTime ||
//             (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : 0)
//         ),
//       };
//     });

//     const totalPaidGross = payments.reduce((s, p) => s + p.amount, 0);

//     // ---------- REFUNDS (ORDERED) ----------
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const refunds = refundsSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         return {
//           id: doc.id,
//           amount: Number(d.amount || 0),
//           date: new Date(
//             d.refundDateTime ||
//               (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : 0)
//           ),
//         };
//       })
//       .sort((a, b) => a.date - b.date);

//     // ---------- REFUND TILL THIS RECEIPT ----------
//     let refundedTillThis = 0;
//     for (const r of refunds) {
//       refundedTillThis += r.amount;
//       if (r.id === id) break;
//     }

//     // ---------- FINAL NUMBERS (FIXED WITH PROCEDURE CHECK) ----------
//     const netPaidTillThis = totalPaidGross - refundedTillThis;
    
//     // 🔥 BUSINESS OVERRIDE: Procedure done = balance ZERO
//     const balanceAfterThis = isProcedureCompleted 
//       ? 0 
//       : Math.max(0, billTotal - netPaidTillThis);

//     // ---------- HELPERS ----------
//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // Assuming you already have these utility functions somewhere
//     // formatDateDot, getClinicProfile, profileValue, etc.

//     const patientName = bill.patientName || "";
//     const address = bill.address || "";
//     const drawnOn = refund.drawnOn || null;
//     const drawnAs = refund.drawnAs || null;
//     const mode = refund.mode || "Cash";
//     const referenceNo = refund.referenceNo || null;
//     const refundNo =
//       refund.refundReceiptNo || `F-${String(id).padStart(4, "0")}`;

//     const chequeDate = refund.chequeDate || null;
//     const chequeNumber = refund.chequeNumber || null;
//     const bankName = refund.bankName || null;
//     const transferType = refund.transferType || null;
//     const transferDate = refund.transferDate || null;
//     const upiName = refund.upiName || null;
//     const upiId = refund.upiId || null;
//     const upiDate = refund.upiDate || null;

//     // ---------- CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // ---------- PDF HEADERS ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="refund-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 40;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftLogoX = 36;
//     const rightLogoX = pageWidth - 36;

//     // HEADER
//     try {
//       doc.image(logoLeftPath, leftLogoX, y, { width: 32, height: 32 });
//     } catch (e) {}
//     try {
//       doc.image(logoRightPath, rightLogoX - 32, y, { width: 32, height: 32 });
//     } catch (e) {}

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(13)
//       .text(clinicName || "", 0, y + 2, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 20, {
//         align: "center",
//         width: pageWidth,
//       })
//       .text(
//         (clinicPAN ? `PAN: ${clinicPAN}` : "") +
//           (clinicPAN && clinicRegNo ? "   |   " : "") +
//           (clinicRegNo ? `Reg. No.: ${clinicRegNo}` : ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 6;

//     // DOCTOR LINE
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(
//       doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
//       pageWidth / 2,
//       y,
//       {
//         align: "right",
//         width: usableWidth / 2,
//       }
//     );

//     y += 16;

//     // TITLE BAR
//     doc
//       .save()
//       .rect(36, y, usableWidth, 18)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 18)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(10).text("REFUND RECEIPT", 36, y + 4, {
//       align: "center",
//       width: usableWidth,
//     });

//     y += 26;

//     // COMMON LAYOUT
//     const leftX = 36;
//     const rightBoxWidth = 180;
//     const rightX = pageWidth - 36 - rightBoxWidth;

//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Refund No.: ${refundNo}`, leftX, y);
//     doc.text(`Date: ${formatDateDot(refund.refundDate || "")}`, rightX, y, {
//       width: rightBoxWidth,
//       align: "right",
//     });

//     y += 16;

//     const detailsTopY = y;
//     const leftWidth = rightX - leftX - 10;

//     doc
//       .font("Helvetica-Bold")
//       .text(`Patient Name: ${patientName}`, leftX, detailsTopY, {
//         width: leftWidth,
//       });
//     doc.font("Helvetica").text(`Address: ${address}`, leftX, doc.y + 3, {
//       width: leftWidth,
//     });

//     let detailsY = doc.y + 4;
//     doc.font("Helvetica");

//     const addDetailR = (label, value) => {
//       if (!value) return;
//       doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
//       detailsY = doc.y + 3;
//     };

//     addDetailR("Amount Refunded: Rs", formatMoney(refund.amount));
//     addDetailR("Refund Mode:", mode);
//     addDetailR("Reference No.:", referenceNo);
//     addDetailR("Drawn On:", drawnOn);
//     addDetailR("Drawn As:", drawnAs);
//     addDetailR("Cheque No.:", chequeNumber);
//     addDetailR("Cheque Date:", formatDateDot(chequeDate));
//     addDetailR("Bank:", bankName);
//     addDetailR("Transfer Type:", transferType);
//     addDetailR("Transfer Date:", formatDateDot(transferDate));
//     addDetailR("UPI ID:", upiId);
//     addDetailR("UPI Name:", upiName);
//     addDetailR("UPI Date:", formatDateDot(upiDate));

//     // RIGHT BILL SUMMARY
//     const boxY = detailsTopY;
//     const lineH = 12;
//     const boxHeight = 100;

//     doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

//     let by2 = boxY + 4;
//     doc.font("Helvetica-Bold").fontSize(9).text("Bill Summary", rightX + 6, by2);
//     by2 += lineH + 2;
//     doc.font("Helvetica").fontSize(9);

//     const billNoText2 = bill.invoiceNo || billId;

//     const addRow2 = (label, value) => {
//       doc.text(label, rightX + 6, by2);
//       doc.text(value, rightX + 6, by2, {
//         width: rightBoxWidth - 12,
//         align: "right",
//       });
//       by2 += lineH;
//     };

//     addRow2("Bill No.:", billNoText2);
//     addRow2("Bill Date:", formatDateDot(bill.date || ""));
//     addRow2("Bill Total:", `Rs ${formatMoney(billTotal)}`);
//     addRow2("Total Paid:", `Rs ${formatMoney(netPaidTillThis)}`);
//     addRow2("Refunded (incl. this):", `Rs ${formatMoney(refund.amount)}`);
//     addRow2("Balance:", `Rs ${formatMoney(balanceAfterThis)}`); // ✅ NOW USES UPDATED BALANCE

//     // FOOTNOTE + SIGNATURES
//     y = Math.max(detailsY + 6, boxY + boxHeight + 6);

//     doc
//       .font("Helvetica")
//       .fontSize(8)
//       .text("* Dispute if any subject to Jamshedpur Jurisdiction", leftX, y, {
//         width: usableWidth,
//       });

//     const sigY = y + 24;
//     const sigWidth = 160;

//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     doc.end();
//   } catch (err) {
//     console.error("refund-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate refund PDF" });
//     }
//   }
// });

app.get("/api/refunds/:id/refund-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid refund id" });

  try {
    // ---------- REFUND ----------
    const refundRef = db.collection("refunds").doc(id);
    const refundSnap = await refundRef.get();
    if (!refundSnap.exists) {
      return res.status(404).json({ error: "Refund not found" });
    }
    const refund = refundSnap.data();
    const billId = refund.billId;

    // ---------- BILL ----------
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();
    const billTotal = Number(bill.total || 0);
    const isProcedureCompleted = bill.procedureConfirmed === true;

    // ---------- PAYMENTS (GROSS) ----------
    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();

    const payments = paysSnap.docs.map((doc) => {
      const d = doc.data();
      return {
        amount: Number(d.amount || 0),
        date: new Date(
          d.paymentDateTime ||
            (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : 0)
        ),
      };
    });

    const totalPaidGross = payments.reduce((s, p) => s + p.amount, 0);

    // ---------- REFUNDS (ORDERED) ----------
    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", billId)
      .get();

    const refunds = refundsSnap.docs
      .map((doc) => {
        const d = doc.data();
        return {
          id: doc.id,
          amount: Number(d.amount || 0),
          date: new Date(
            d.refundDateTime ||
              (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : 0)
          ),
        };
      })
      .sort((a, b) => a.date - b.date);

    // ---------- REFUND TILL THIS RECEIPT ----------
    let refundedTillThis = 0;
    for (const r of refunds) {
      refundedTillThis += r.amount;
      if (r.id === id) break;
    }

    // ---------- FINAL NUMBERS ----------
    const netPaidTillThis = totalPaidGross - refundedTillThis;
    
    // 🔥 BUSINESS OVERRIDE: Procedure done = balance ZERO
    const balanceAfterThis = isProcedureCompleted 
      ? 0 
      : Math.max(0, billTotal - netPaidTillThis);

    // ---------- HELPERS ----------
    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const patientName = bill.patientName || "";
    const address = bill.address || "";
    const drawnOn = refund.drawnOn || null;
    const drawnAs = refund.drawnAs || null;
    const mode = refund.mode || "Cash";
    const referenceNo = refund.referenceNo || null;
    const refundNo =
      refund.refundReceiptNo || `F-${String(id).padStart(4, "0")}`;

    const chequeDate = refund.chequeDate || null;
    const chequeNumber = refund.chequeNumber || null;
    const bankName = refund.bankName || null;
    const transferType = refund.transferType || null;
    const transferDate = refund.transferDate || null;
    const upiName = refund.upiName || null;
    const upiId = refund.upiId || null;
    const upiDate = refund.upiDate || null;

    // ---------- CLINIC PROFILE ----------
    const profile = await getClinicProfile({ force: true });
    const clinicName = profileValue(profile, "clinicName");
    const clinicAddress = profileValue(profile, "address");
    const clinicPAN = profileValue(profile, "pan");
    const clinicRegNo = profileValue(profile, "regNo");
    const doctor1Name = profileValue(profile, "doctor1Name");
    const doctor1RegNo = profileValue(profile, "doctor1RegNo");
    const doctor2Name = profileValue(profile, "doctor2Name");
    const doctor2RegNo = profileValue(profile, "doctor2RegNo");
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

    // ---------- PDF HEADERS ----------
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="refund-${id}.pdf"`
    );

    const doc = new PDFDocument({
      size: "A4",
      margin: 36,
    });

    doc.pipe(res);

    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72;
    let y = 40;

    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

    const leftLogoX = 36;
    const rightLogoX = pageWidth - 36;

    // HEADER
    try {
      doc.image(logoLeftPath, leftLogoX, y, { width: 32, height: 32 });
    } catch (e) {}
    try {
      doc.image(logoRightPath, rightLogoX - 32, y, { width: 32, height: 32 });
    } catch (e) {}

    doc
      .font("Helvetica-Bold")
      .fontSize(13)
      .text(clinicName || "", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(clinicAddress || "", 0, y + 20, {
        align: "center",
        width: pageWidth,
      })
      .text(
        (clinicPAN ? `PAN: ${clinicPAN}` : "") +
          (clinicPAN && clinicRegNo ? "   |   " : "") +
          (clinicRegNo ? `Reg. No.: ${clinicRegNo}` : ""),
        {
          align: "center",
          width: pageWidth,
        }
      );

    y += 48;

    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
    y += 6;

    // DOCTOR LINE
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text(doctor1Name || "", 36, y);
    doc.text(doctor2Name || "", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
    doc.text(
      doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );

    y += 16;

    // TITLE BAR
    doc
      .save()
      .rect(36, y, usableWidth, 18)
      .fill("#F3F3F3")
      .restore()
      .rect(36, y, usableWidth, 18)
      .stroke();

    doc.font("Helvetica-Bold").fontSize(10).text("REFUND RECEIPT", 36, y + 4, {
      align: "center",
      width: usableWidth,
    });

    y += 26;

    // COMMON LAYOUT
    const leftX = 36;
    const rightBoxWidth = 180;
    const rightX = pageWidth - 36 - rightBoxWidth;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Refund No.: ${refundNo}`, leftX, y);
    doc.text(`Date: ${formatDateDot(refund.refundDate || "")}`, rightX, y, {
      width: rightBoxWidth,
      align: "right",
    });

    y += 16;

    const detailsTopY = y;
    const leftWidth = rightX - leftX - 10;

    doc
      .font("Helvetica-Bold")
      .text(`Patient Name: ${patientName}`, leftX, detailsTopY, {
        width: leftWidth,
      });
    doc.font("Helvetica").text(`Address: ${address}`, leftX, doc.y + 3, {
      width: leftWidth,
    });

    let detailsY = doc.y + 4;
    doc.font("Helvetica");

    const addDetailR = (label, value) => {
      if (!value) return;
      doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
      detailsY = doc.y + 3;
    };

    addDetailR("Amount Refunded: Rs", formatMoney(refund.amount));
    addDetailR("Refund Mode:", mode);
    addDetailR("UTR NO./REF NO.:", referenceNo);
    addDetailR("Drawn On:", drawnOn);
    addDetailR("Drawn As:", drawnAs);
    addDetailR("Cheque No.:", chequeNumber);
    addDetailR("Cheque Date:", formatDateDot(chequeDate));
    addDetailR("Bank:", bankName);
    addDetailR("Transfer Type:", transferType);
    addDetailR("Transfer Date:", formatDateDot(transferDate));
    addDetailR("UPI ID:", upiId);
    addDetailR("UPI Name:", upiName);
    addDetailR("UPI Date:", formatDateDot(upiDate));

    // RIGHT BILL SUMMARY
    const boxY = detailsTopY;
    const lineH = 12;
    const boxHeight = 100;

    doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

    let by2 = boxY + 4;
    doc.font("Helvetica-Bold").fontSize(9).text("Bill Summary", rightX + 6, by2);
    by2 += lineH + 2;
    doc.font("Helvetica").fontSize(9);

    const billNoText2 = bill.invoiceNo || billId;

    const addRow2 = (label, value) => {
      doc.text(label, rightX + 6, by2);
      doc.text(value, rightX + 6, by2, {
        width: rightBoxWidth - 12,
        align: "right",
      });
      by2 += lineH;
    };

    addRow2("Bill No.:", billNoText2);
    addRow2("Bill Date:", formatDateDot(bill.date || ""));
    addRow2("Bill Total:", `Rs ${formatMoney(billTotal)}`);
    addRow2("Total Paid:", `Rs ${formatMoney(netPaidTillThis)}`);
    addRow2("Refund Amount:", `Rs ${formatMoney(refundedTillThis)}`); // ✅ CHANGED: Show total refunded
    addRow2("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

    // FOOTNOTE + SIGNATURES
    y = Math.max(detailsY + 6, boxY + boxHeight + 6);

    doc
      .font("Helvetica")
      .fontSize(8)
      .text("* Dispute if any subject to Jamshedpur Jurisdiction", leftX, y, {
        width: usableWidth,
      });

    const sigY = y + 24;
    const sigWidth = 160;

    const rightSigX = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    doc.end();
  } catch (err) {
    console.error("refund-html-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate refund PDF" });
    }
  }
});


app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  function formatMoney(v) {
    return Number(v || 0).toFixed(2);
  }

  function formatDateOnly(dtString) {
    return typeof formatDateDot === "function"
      ? formatDateDot(dtString)
      : dtString || "";
  }

  try {
    // ---------- LOAD BILL ----------
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists)
      return res.status(404).json({ error: "Bill not found" });
    const bill = billSnap.data();

    // fetch items (legacy/new combined)
    const itemsSnap = await db
      .collection("items")
      .where("billId", "==", id)
      .get();
    const legacyItems = itemsSnap.docs.map((d) => {
      const dd = d.data();
      return {
        id: d.id,
        description: dd.description || dd.item || dd.details || "",
        qty: Number(dd.qty || 0),
        rate: Number(dd.rate || 0),
        amount:
          dd.amount != null
            ? Number(dd.amount)
            : Number(dd.qty || 0) * Number(dd.rate || 0),
      };
    });

    const serviceItems = Array.isArray(bill.services)
      ? bill.services.map((s, idx) => {
          const qty = Number(s.qty || 0);
          const rate = Number(s.rate || 0);
          const amount = s.amount != null ? Number(s.amount) : qty * rate;
          const parts = [];
          if (s.item) parts.push(s.item);
          if (s.details) parts.push(s.details);
          return {
            id: `svc-${idx + 1}`,
            description: parts.join(" - "),
            qty,
            rate,
            amount,
          };
        })
      : [];
    const items = serviceItems.length > 0 ? serviceItems : legacyItems;

    // payments & refunds
    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", id)
      .get();
    const payments = paysSnap.docs.map((d) => {
      const pd = d.data();
      return {
        id: d.id,
        type: "Payment",
        paymentDateTime:
          pd.paymentDateTime ||
          (pd.paymentDate
            ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z`
            : null),
        paymentDate: pd.paymentDate || null,
        paymentTime: pd.paymentTime || null,
        amount: Number(pd.amount || 0),
        mode: pd.mode || "",
        referenceNo: pd.referenceNo || "",
        chequeDate: pd.chequeDate || null,
        chequeNumber: pd.chequeNumber || null,
        bankName: pd.bankName || null,
        transferType: pd.transferType || null,
        transferDate: pd.transferDate || null,
        upiName: pd.upiName || null,
        upiId: pd.upiId || null,
        upiDate: pd.upiDate || null,
        drawnOn: pd.drawnOn || null,
        drawnAs: pd.drawnAs || null,
        receiptNo: pd.receiptNo || d.id,
      };
    });

    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", id)
      .get();
    // const refunds = refundsSnap.docs.map((d) => {
    //   const rd = d.data();
    //   return {
    //     id: d.id,
    //     type: "Refund",
    //     paymentDateTime:
    //       rd.refundDateTime ||
    //       (rd.refundDate
    //         ? `${rd.refundDate}T${rd.refundTime || "00:00"}:00.000Z`
    //         : null),
    //     paymentDate: rd.refundDate || null,
    //     paymentTime: rd.refundTime || null,
    //     amount: Number(rd.amount || 0),
    //     mode: rd.mode || "",
    //     referenceNo: rd.referenceNo || "",
    //     chequeDate: rd.chequeDate || null,
    //     chequeNumber: rd.chequeNumber || null,
    //     bankName: rd.bankName || null,
    //     transferType: rd.transferType || null,
    //     transferDate: rd.transferDate || null,
    //     upiName: rd.upiName || null,
    //     upiId: rd.upiId || null,
    //     upiDate: rd.upiDate || null,
    //     receiptNo: rd.refundNo || d.id,
    //   };
    // });

    // Combine payments and refunds, then sort chronologically
    
    const refunds = refundsSnap.docs.map((d) => {
      const rd = d.data();
      return {
        id: d.id,
        type: "Refund",
        paymentDateTime:
          rd.refundDateTime ||
          (rd.refundDate
            ? `${rd.refundDate}T${rd.refundTime || "00:00"}:00.000Z`
            : null),
        paymentDate: rd.refundDate || null,
        paymentTime: rd.refundTime || null,
        amount: Number(rd.amount || 0),
        mode: rd.mode || "",
        referenceNo: rd.referenceNo || "",
        chequeDate: rd.chequeDate || null,
        chequeNumber: rd.chequeNumber || null,
        bankName: rd.bankName || null,
        transferType: rd.transferType || null,
        transferDate: rd.transferDate || null,
        upiName: rd.upiName || null,
        upiId: rd.upiId || null,
        upiDate: rd.upiDate || null,
        receiptNo: (rd.refundNo || d.id).replace(/_/g, '/'),
      };
    });
    
    const allTransactions = [...payments, ...refunds];
    allTransactions.sort((a, b) => {
      const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
      const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
      return da - dbb;
    });

    // totals
    const total = Number(
      bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0)
    );
    const totalPaidGross = payments.reduce(
      (s, p) => s + Number(p.amount || 0),
      0
    );
    const totalRefunded = refunds.reduce(
      (s, r) => s + Number(r.amount || 0),
      0
    );
    const netPaid = totalPaidGross - totalRefunded;
    const balance = total - netPaid;

    // Only allow full-payment PDF if balance is zero (or less)
    // if (balance > 0) {
    //   return res.status(400).json({
    //     error:
    //       "Bill not fully paid - full payment PDF is available only after full payment",
    //   });
    // }

    // ---------- FETCH CLINIC PROFILE ----------
    const profile = await getClinicProfile({ force: true });
    const clinicName = profileValue(profile, "clinicName");
    const clinicAddress = profileValue(profile, "address");
    const clinicPAN = profileValue(profile, "pan");
    const clinicRegNo = profileValue(profile, "regNo");
    const doctor1Name = profileValue(profile, "doctor1Name");
    const doctor1RegNo = profileValue(profile, "doctor1RegNo");
    const doctor2Name = profileValue(profile, "doctor2Name");
    const doctor2RegNo = profileValue(profile, "doctor2RegNo");
    const patientRepresentative = profileValue(
      profile,
      "patientRepresentative"
    );
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

    // --- PDF Setup ---
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="full-payment-${id}.pdf"`
    );

    const doc = new PDFDocument({ size: "A4", margin: 0 });
    doc.pipe(res);

    const pageMargin = 15;
    const borderPadding = 20;

    function computeContentArea() {
      const pw = doc.page.width;
      const ph = doc.page.height;
      const contentLeft = pageMargin + borderPadding;
      const contentTop = pageMargin + borderPadding;
      const contentRight = pw - (pageMargin + borderPadding);
      const contentBottom = ph - (pageMargin + borderPadding);
      const usableWidth = contentRight - contentLeft;
      const usableHeight = contentBottom - contentTop;
      return {
        contentLeft,
        contentTop,
        contentRight,
        contentBottom,
        usableWidth,
        usableHeight,
      };
    }

    function drawPageBorder() {
      try {
        const pw = doc.page.width;
        const ph = doc.page.height;
        doc.save();
        doc.lineWidth(0.8);
        doc
          .rect(
            pageMargin,
            pageMargin,
            pw - pageMargin * 2,
            ph - pageMargin * 2
          )
          .stroke();
        doc.restore();
      } catch (e) {
        /* ignore */
      }
    }

    // fonts
    try {
      const workSansPath = path.join(
        __dirname,
        "resources",
        "WorkSans-Regular.ttf"
      );
      if (fs && fs.existsSync(workSansPath)) {
        doc.registerFont("WorkSans", workSansPath);
        doc.font("WorkSans");
      } else {
        doc.font("Helvetica");
      }
    } catch (e) {
      doc.font("Helvetica");
    }

    // initial
    drawPageBorder();
    let { contentLeft, contentTop, contentRight, usableWidth } =
      computeContentArea();
    let y = contentTop;

    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

    function drawPageHeader() {
      const ca = computeContentArea();
      contentLeft = ca.contentLeft;
      contentTop = ca.contentTop;
      contentRight = ca.contentRight;
      usableWidth = ca.usableWidth;

      const logoW = 40;
      const logoH = 40;
      try {
        if (fs && fs.existsSync(logoLeftPath))
          doc.image(logoLeftPath, contentLeft, y, {
            width: logoW,
            height: logoH,
          });
      } catch (e) {}
      try {
        if (fs && fs.existsSync(logoRightPath))
          doc.image(logoRightPath, contentRight - logoW, y, {
            width: logoW,
            height: logoH,
          });
      } catch (e) {}

      doc
        .fontSize(14)
        .font("Helvetica-Bold")
        .text(clinicName || "", contentLeft, y + 6, {
          width: usableWidth,
          align: "center",
        });
      doc
        .fontSize(9)
        .font("Helvetica")
        .text(clinicAddress || "", contentLeft, y + 26, {
          width: usableWidth,
          align: "center",
        });
      doc.text(
        `PAN : ${clinicPAN || ""}   |   Reg. No: ${clinicRegNo || ""}`,
        contentLeft,
        y + 40,
        { width: usableWidth, align: "center" }
      );

      y += 56;
      doc.moveTo(contentLeft, y).lineTo(contentRight, y).stroke();
      y += 8;
    }

    drawPageHeader();

    // --- doctors, invoice title, patient info ---
    doc.fontSize(9).font("Helvetica-Bold");
    doc.text(doctor1Name || "", contentLeft, y);
    doc.text(doctor2Name || "", contentLeft, y, {
      width: usableWidth,
      align: "right",
    });
    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", contentLeft, y);
    doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", contentLeft, y, {
      width: usableWidth,
      align: "right",
    });
    y += 18;

    doc
      .fontSize(10)
      .font("Helvetica-Bold")
      .rect(contentLeft, y, usableWidth, 18)
      .stroke();
    doc.text("INVOICE CUM PAYMENT RECEIPT", contentLeft, y + 4, {
      width: usableWidth,
      align: "center",
    });
    y += 28;

    const invoiceNo = bill.invoiceNo || id;
    const dateText = formatDateOnly(bill.date || "");
    doc.fontSize(9).font("Helvetica-Bold");
    doc.text(`Invoice No.: ${invoiceNo}`, contentLeft, y);
    doc.text(`Date: ${dateText}`, contentLeft, y, {
      width: usableWidth,
      align: "right",
    });
    y += 14;

    // Patient info
    const patientName = bill.patientName || "";
    const sexText = bill.sex ? String(bill.sex) : "";
    const ageText =
      bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
    const addressText = bill.address || "";
    const procedureText = bill.procedureDone || "";

    doc
      .font("Helvetica-Bold")
      .text(`Patient Name: ${patientName}`, contentLeft, y);
    y += 14;

    doc.font("Helvetica");
    if (sexText) {
      doc.text(
        `Address: ${addressText || "____________________"}`,
        contentLeft,
        y,
        {
          width: usableWidth * 0.6,
        }
      );
      y += 20;
    } else {
      doc.text(
        `Address: ${addressText || "____________________"}`,
        contentLeft,
        y,
        {
          width: usableWidth,
        }
      );
      y += 20;
    }

    // Procedure Done
    doc
      .fontSize(9)
      .font("Helvetica")
      .text(
        `Procedure Done :- ${procedureText || "____________________"}`,
        contentLeft,
        y
      );

    y += 14;

    // ---------- SERVICES / ITEMS TABLE ----------
    const tableLeft = contentLeft;
    const colSrW = 24;
    const colQtyW = 48;
    const colRateW = 70;
    const colSubW = 80;
    const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colSubW);
    const colSrX = tableLeft;
    const colServiceX = colSrX + colSrW;
    const colQtyX = colServiceX + colServiceW;
    const colRateX = colQtyX + colQtyW;
    const colSubX = colRateX + colRateW;
    const tableRightX = tableLeft + usableWidth;

    const headerHeight = 14;
    const headerPadding = 3;
    const headerDrawH = headerHeight + headerPadding * 2;
    const minRowH = 18;
    const minTableHeight = 180;
    const bottomSafety = 100;

    function drawVerticalsForItemsBlock(yTop, height) {
      const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
      xs.forEach((x) => {
        doc
          .moveTo(x, yTop)
          .lineTo(x, yTop + height)
          .stroke();
      });
    }

    let tableStartY = y;

    // header background + text
    doc
      .save()
      .rect(tableLeft, y, usableWidth, headerDrawH)
      .fill("#F3F3F3")
      .restore();
    doc.font("Helvetica-Bold").fontSize(9);
    const headerTextYOffset = headerPadding + 3;
    doc.text("Sr.", colSrX , y + headerTextYOffset, {
      width: colSrW,
      align: "center",
    });
    doc.text(
      "Description of Items / Services",
      colServiceX + 4,
      y + headerTextYOffset,
      { width: colServiceW - 8,
        align: "center"
       }
    );
    doc.text("Qty", colQtyX, y + headerTextYOffset,{
      width: colQtyW,
      align: "center",
    });
    doc.text("Rate", colRateX + 4, y + headerTextYOffset, {
      width: colRateW - 8,
      align: "center",
    });
    doc.text("Amount", colSubX + 4, y + headerTextYOffset, {
      width: colSubW - 8,
      align: "center",
    });

    y += headerDrawH;
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // render items
    doc.font("Helvetica").fontSize(10);
    let filledHeight = 0;
    for (let i = 0; i < items.length; i++) {
      const it = items[i];

      const descH = doc.heightOfString(it.description || "", {
        width: colServiceW - 8,
      });
      const qtyH = doc.heightOfString(String(it.qty || ""), {
        width: colQtyW - 8,
      });
      const rateH = doc.heightOfString(formatMoney(it.rate || 0), {
        width: colRateW - 8,
      });
      const amtH = doc.heightOfString(formatMoney(it.amount || 0), {
        width: colSubW - 8,
      });

      const contentMaxH = Math.max(descH, qtyH, rateH, amtH);
      const thisRowH = Math.max(minRowH, contentMaxH + 6);

      // page-break check
      if (y + thisRowH > doc.page.height - bottomSafety) {
        const visualHeightSoFar = headerDrawH + filledHeight;
        if (visualHeightSoFar > 0) {
          drawVerticalsForItemsBlock(tableStartY, visualHeightSoFar);
          doc
            .rect(tableLeft, tableStartY, usableWidth, visualHeightSoFar)
            .stroke();
        }

        doc.addPage();
        drawPageBorder();
        ({ contentLeft, contentTop, contentRight, usableWidth } =
          computeContentArea());
        y = contentTop;

        drawPageHeader();

        doc
          .fontSize(10)
          .font("Helvetica-Bold")
          .rect(contentLeft, y, usableWidth, 18)
          .stroke();
        doc.text("INVOICE CUM PAYMENT RECEIPT", contentLeft, y + 4, {
          width: usableWidth,
          align: "center",
        });
        y += 28;

        tableStartY = y;
        doc
          .save()
          .rect(tableLeft, y, usableWidth, headerDrawH)
          .fill("#F3F3F3")
          .restore();
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Sr.", colSrX + 4, y + headerTextYOffset);
        doc.text(
          "Description of Items / Services",
          colServiceX + 4,
          y + headerTextYOffset,
          { width: colServiceW - 8 }
        );
        doc.text("Qty", colQtyX + 4, y + headerTextYOffset);
        doc.text("Rate", colRateX + 4, y + headerTextYOffset, {
          width: colRateW - 8,
          align: "right",
        });
        doc.text("Amount", colSubX + 4, y + headerTextYOffset, {
          width: colSubW - 8,
          align: "right",
        });
        y += headerDrawH;
        doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

        filledHeight = 0;
      }

      const descH2 = doc.heightOfString(it.description || "", {
        width: colServiceW - 8,
      });
      const qtyH2 = doc.heightOfString(String(it.qty || ""), {
        width: colQtyW - 8,
      });
      const rateH2 = doc.heightOfString(formatMoney(it.rate || 0), {
        width: colRateW - 8,
      });
      const amtH2 = doc.heightOfString(formatMoney(it.amount || 0), {
        width: colSubW - 8,
      });
      const actualRowH = Math.max(
        minRowH,
        Math.max(descH2, qtyH2, rateH2, amtH2) + 6
      );

      const rowTop = y;
      const descY = rowTop + (actualRowH - descH2) / 2;
      const qtyY = rowTop + (actualRowH - qtyH2) / 2;
      const rateY = rowTop + (actualRowH - rateH2) / 2;
      const amtY = rowTop + (actualRowH - amtH2) / 2;

      doc.text(String(i + 1), colSrX + 4, rowTop + 3);
      doc.text(it.description || "", colServiceX + 4, descY, {
        width: colServiceW - 8,
      });
      doc.text(
        String(it.qty != null && it.qty !== "" ? it.qty : ""),
        colQtyX + 4,
        qtyY,
        { width: colQtyW - 8, align: "left" }
      );
      doc.text(formatMoney(it.rate || 0), colRateX + 4, rateY, {
        width: colRateW - 8,
        align: "right",
      });
      doc.text(formatMoney(it.amount || 0), colSubX + 4, amtY, {
        width: colSubW - 8,
        align: "right",
      });

      y += actualRowH;
      filledHeight += actualRowH;
    }

    // ensure minimum visual height of the items block (filler rows)
    let totalTableHeight = filledHeight;
    if (totalTableHeight < minTableHeight) {
      let needed = minTableHeight - totalTableHeight;
      const fillerRows = Math.ceil(needed / minRowH);
      for (let fr = 0; fr < fillerRows; fr++) {
        if (y + minRowH > doc.page.height - bottomSafety) {
          drawVerticalsForItemsBlock(
            tableStartY,
            headerDrawH + totalTableHeight
          );
          doc
            .rect(
              tableLeft,
              tableStartY,
              usableWidth,
              headerDrawH + totalTableHeight
            )
            .stroke();

          doc.addPage();
          drawPageBorder();
          ({ contentLeft, contentTop, contentRight, usableWidth } =
            computeContentArea());
          y = contentTop;

          drawPageHeader();
          doc
            .fontSize(10)
            .font("Helvetica-Bold")
            .rect(contentLeft, y, usableWidth, 18)
            .stroke();
          doc.text("INVOICE CUM PAYMENT RECEIPT", contentLeft, y + 4, {
            width: usableWidth,
            align: "center",
          });
          y += 28;

          tableStartY = y;
          doc
            .save()
            .rect(tableLeft, y, usableWidth, headerDrawH)
            .fill("#F3F3F3")
            .restore();
          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Sr.", colSrX + 4, y + headerTextYOffset);
          doc.text(
            "Description of Items / Services",
            colServiceX + 4,
            y + headerTextYOffset,
            { width: colServiceW - 8 }
          );
          doc.text("Qty", colQtyX + 4, y + headerTextYOffset);
          doc.text("Rate", colRateX + 4, y + headerTextYOffset, {
            width: colRateW - 8,
            align: "right",
          });
          doc.text("Amount", colSubX + 4, y + headerTextYOffset, {
            width: colSubW - 8,
            align: "right",
          });
          y += headerDrawH;
          doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

          totalTableHeight = 0;
        }

        y += minRowH;
        totalTableHeight += minRowH;
      }
      filledHeight = totalTableHeight;
    }

    // draw vertical separators spanning header + content
    const visualTableHeight = headerDrawH + filledHeight;
    drawVerticalsForItemsBlock(tableStartY, visualTableHeight);
    doc.rect(tableLeft, tableStartY, usableWidth, visualTableHeight).stroke();

    // ===== Net Paid box after items =====
    const netBoxW = 120;
    const netBoxH = 28;
    const netBoxX = tableRightX - netBoxW;
    let netBoxY = tableStartY + visualTableHeight;

    if (netBoxY + netBoxH + 60 > doc.page.height) {
      doc.addPage();
      drawPageBorder();
      ({ contentLeft, contentTop, contentRight, usableWidth } =
        computeContentArea());
      y = contentTop;
      drawPageHeader();
      netBoxY = contentTop + 54;
    }

    doc.rect(netBoxX, netBoxY, netBoxW, netBoxH).stroke();
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .text("Total", netBoxX + 6, netBoxY + 6, {
        width: netBoxW - 12,
        align: "left",
      });
    doc
      .font("Helvetica")
      .fontSize(9)
      .text(`Rs ${formatMoney(netPaid)}`, netBoxX + 6, netBoxY + 6, {
        width: netBoxW - 12,
        align: "right",
      });

    if (netBoxY === tableStartY + visualTableHeight) {
      y = netBoxY + netBoxH + 8;
    } else {
      y = netBoxY + netBoxH + 12;
    }

    // ---------- PAYMENT DETAILS HEADING ----------
    doc.moveDown(0.2);
    doc
      .fontSize(10)
      .font("Helvetica-Bold")
      .text("Payment Details", contentLeft, y);
    y += 16;

    // ---------- PAYMENT DETAILS TABLE (with Type column added) ----------
    const pTableLeft = contentLeft;

    const pColDateW = 55;
    const pColRecW = 100;
    const pColTypeW = 45;
    const pColModeW = 50;
    const pColBankW = 90;
    const pColRefW = 135;
    const pColAmtW =
      usableWidth -
      (pColDateW + pColRecW + pColTypeW + pColModeW + pColBankW + pColRefW);

    const pColDateX = pTableLeft;
    const pColRecX = pColDateX + pColDateW;
    const pColTypeX = pColRecX + pColRecW;
    const pColModeX = pColTypeX + pColTypeW;
    const pColBankX = pColModeX + pColModeW;
    const pColRefX = pColBankX + pColBankW;
    const pColAmtX = pColRefX + pColRefW;
    const pTableRightX = pTableLeft + usableWidth;

    const pHeaderH = 14;
    const pHeaderDrawH = pHeaderH + headerPadding * 2;
    const pMinRowH = 12;
    const pMinTableH = 90;
    const pBottomSafety = 100;

    function drawVerticalsForPaymentsBlock(yTop, height) {
      const xs = [
        pColDateX,
        pColRecX,
        pColTypeX,
        pColModeX,
        pColBankX,
        pColRefX,
        pColAmtX,
        pTableRightX,
      ];
      xs.forEach((x) => {
        doc
          .moveTo(x, yTop)
          .lineTo(x, yTop + height)
          .stroke();
      });
    }

    // payments table header placement
    let pTableStartY = y;
    doc
      .save()
      .rect(pTableLeft, y, usableWidth, pHeaderDrawH)
      .fill("#F3F3F3")
      .restore();
    doc.font("Helvetica-Bold").fontSize(9);
    const pHeaderTextYOffset = headerPadding + 3;

    doc.text("Date", pColDateX + 4, y + pHeaderTextYOffset, {
      width: pColDateW - 4,
      align: "center",
    });
    doc.text("Receipt / Refund No.", pColRecX + 4, y + pHeaderTextYOffset, {
      width: pColRecW - 8,
      align: "center",
    });
    doc.text("Type", pColTypeX, y + pHeaderTextYOffset, {
      width: pColTypeW - 4,
      align: "center",
    });
    doc.text("Mode", pColModeX , y + pHeaderTextYOffset, {
      width: pColModeW - 4,
      align: "center",
    });
    doc.text("Bank Name / UPI ID", pColBankX, y + pHeaderTextYOffset, {
      width: pColBankW - 4,
      align: "center",
    });
    doc.text("Cheque No./UTR No./REF No.", pColRefX , y + pHeaderTextYOffset, {
      width: pColRefW - 4,
      align: "center",
    });
    doc.text("Amount", pColAmtX + 4, y + pHeaderTextYOffset, {
      width: pColAmtW - 4,
      align: "center",

    });

    y += pHeaderDrawH;
    doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

    // render payments and refunds rows
    doc.font("Helvetica").fontSize(9);
    let pFilledHeight = 0;
    for (let i = 0; i < allTransactions.length; i++) {
      const p = allTransactions[i];

      const dateText = formatDateOnly(p.paymentDate || "");
      const receiptText = p.receiptNo || p.id || "";
      const typeText = p.type || "Payment";
      
      let modeText = p.mode || "-";
      if (
        (modeText === "BankTransfer" ||
          (modeText && modeText.toLowerCase().includes("bank"))) &&
        p.transferType
      ) {
        modeText = `Bank (${p.transferType})`;
      }
      
      let bankText = "-";
      let refText = "-";

      if (p.mode === "Cheque") {
        refText = p.chequeNumber || p.referenceNo || "-";
        bankText = p.bankName || "-";
      } else if (p.mode === "UPI") {
        bankText = p.upiId || "-";
        refText = p.referenceNo || "-";
      } else {
        bankText = p.bankName || "-";
        refText = p.referenceNo || "-";
      }

      const amtText = formatMoney(p.amount || 0);

      const dH = doc.heightOfString(dateText, { width: pColDateW - 8 });
      const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
      const tH = doc.heightOfString(typeText, { width: pColTypeW - 8 });
      const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
      const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
      const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
      const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

      const maxH = Math.max(dH, rH, tH, mH, bH, refH, aH);
      const thisRowH = Math.max(pMinRowH, maxH + 6);

      // page-break check
      if (y + thisRowH > doc.page.height - pBottomSafety) {
        const paymentsVisualHeightSoFar = pHeaderDrawH + pFilledHeight;
        if (paymentsVisualHeightSoFar > 0) {
          drawVerticalsForPaymentsBlock(pTableStartY, paymentsVisualHeightSoFar);
          doc
            .rect(
              pTableLeft,
              pTableStartY,
              usableWidth,
              paymentsVisualHeightSoFar
            )
            .stroke();
        }

        doc.addPage();
        drawPageBorder();
        ({ contentLeft, contentTop, contentRight, usableWidth } =
          computeContentArea());
        y = contentTop;

        drawPageHeader();

        pTableStartY = y;
        doc
          .save()
          .rect(pTableLeft, y, usableWidth, pHeaderDrawH)
          .fill("#F3F3F3")
          .restore();
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Date", pColDateX + 4, y + pHeaderTextYOffset, {
          width: pColDateW - 8,
        });
        doc.text("Receipt / Refund No.", pColRecX + 4, y + pHeaderTextYOffset, {
          width: pColRecW - 8,
        });
        doc.text("Type", pColTypeX + 4, y + pHeaderTextYOffset, {
          width: pColTypeW - 8,
        });
        doc.text("Mode", pColModeX + 4, y + pHeaderTextYOffset, {
          width: pColModeW - 8,
        });
        doc.text("Bank Name / UPI ID", pColBankX + 4, y + pHeaderTextYOffset, {
          width: pColBankW - 8,
        });
        doc.text("Cheque No./UTR No./REF No.", pColRefX + 4, y + pHeaderTextYOffset, {
          width: pColRefW - 8,
        });
        doc.text("Amount", pColAmtX + 4, y + pHeaderTextYOffset, {
          width: pColAmtW - 8,
          align: "right",
        });
        y += pHeaderDrawH;
        doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

        pFilledHeight = 0;
      }

      // draw cells
      const rowTop = y;
      const dY = rowTop + (thisRowH - dH) / 2;
      const recY = rowTop + (thisRowH - rH) / 2;
      const typeY = rowTop + (thisRowH - tH) / 2;
      const modeY = rowTop + (thisRowH - mH) / 2;
      const bankY = rowTop + (thisRowH - bH) / 2;
      const refY = rowTop + (thisRowH - refH) / 2;
      const amtY = rowTop + (thisRowH - aH) / 2;

      doc.text(dateText, pColDateX + 4, dY, {
        width: pColDateW - 8,
      });
      doc.text(receiptText, pColRecX + 4, recY, {
        width: pColRecW - 8,
      });
      doc.text(typeText, pColTypeX + 4, typeY, {
        width: pColTypeW - 8,
      });
      doc.text(modeText, pColModeX + 4, modeY, {
        width: pColModeW - 8,
      });
      doc.text(bankText, pColBankX + 4, bankY, {
        width: pColBankW - 8,
      });
      doc.text(refText, pColRefX + 4, refY, {
        width: pColRefW - 8,
      });
      doc.text(amtText, pColAmtX + 4, amtY, {
        width: pColAmtW - 8,
        align: "right",
      });

      y += thisRowH;
      pFilledHeight += thisRowH;
    }

    // ensure payments block minimum visual height (filler rows)
    if (pFilledHeight < pMinTableH) {
      const need = pMinTableH - pFilledHeight;
      const fillerCount = Math.ceil(need / pMinRowH);
      for (let f = 0; f < fillerCount; f++) {
        if (y + pMinRowH > doc.page.height - pBottomSafety) {
          const paymentsVisualHeightSoFar = pHeaderDrawH + pFilledHeight;
          if (paymentsVisualHeightSoFar > 0) {
            drawVerticalsForPaymentsBlock(
              pTableStartY,
              paymentsVisualHeightSoFar
            );
            doc
              .rect(
                pTableLeft,
                pTableStartY,
                usableWidth,
                paymentsVisualHeightSoFar
              )
              .stroke();
          }
          doc.addPage();
          drawPageBorder();
          ({ contentLeft, contentTop, contentRight, usableWidth } =
            computeContentArea());
          y = contentTop;

          drawPageHeader();
          pTableStartY = y;
          doc
            .save()
            .rect(pTableLeft, y, usableWidth, pHeaderDrawH)
            .fill("#F3F3F3")
            .restore();
          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Date", pColDateX + 4, y + pHeaderTextYOffset, {
            width: pColDateW - 8,
          });
          doc.text("Receipt / Refund No.", pColRecX + 4, y + pHeaderTextYOffset, {
            width: pColRecW - 8,
          });
          doc.text("Type", pColTypeX + 4, y + pHeaderTextYOffset, {
            width: pColTypeW - 8,
          });
          doc.text("Mode", pColModeX + 4, y + pHeaderTextYOffset, {
            width: pColModeW - 8,
          });
          doc.text("Bank Name / UPI ID", pColBankX + 4, y + pHeaderTextYOffset, {
            width: pColBankW - 8,
          });
          doc.text("Cheque No./UTR No./REF No.", pColRefX + 4, y + pHeaderTextYOffset, {
            width: pColRefW - 8,
          });
          doc.text("Amount", pColAmtX + 4, y + pHeaderTextYOffset, {
            width: pColAmtW - 8,
            align: "right",
          });
          y += pHeaderDrawH;
          doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

          pFilledHeight = 0;
        }

        y += pMinRowH;
        pFilledHeight += pMinRowH;
      }
    }

    // finalize payments block visuals
    if (pFilledHeight > 0) {
      const paymentsVisualHeight = pHeaderDrawH + pFilledHeight;
      drawVerticalsForPaymentsBlock(pTableStartY, paymentsVisualHeight);
      doc
        .rect(pTableLeft, pTableStartY, usableWidth, paymentsVisualHeight)
        .stroke();
      y = pTableStartY + paymentsVisualHeight + 12;
    }

    // ---------- AMOUNT IN WORDS + NOTE + TOTALS BOX ----------
    // Function to convert number to words
    function numberToWords(num) {
      if (num === 0) return "Zero";
      
      const ones = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"];
      const tens = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"];
      const teens = ["Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"];
      
      function convertLessThanThousand(n) {
        if (n === 0) return "";
        if (n < 10) return ones[n];
        if (n < 20) return teens[n - 10];
        if (n < 100) return tens[Math.floor(n / 10)] + (n % 10 !== 0 ? " " + ones[n % 10] : "");
        return ones[Math.floor(n / 100)] + " Hundred" + (n % 100 !== 0 ? " " + convertLessThanThousand(n % 100) : "");
      }
      
      if (num < 1000) return convertLessThanThousand(num);
      if (num < 100000) {
        const thousands = Math.floor(num / 1000);
        const remainder = num % 1000;
        return convertLessThanThousand(thousands) + " Thousand" + (remainder !== 0 ? " " + convertLessThanThousand(remainder) : "");
      }
      if (num < 10000000) {
        const lakhs = Math.floor(num / 100000);
        const remainder = num % 100000;
        return convertLessThanThousand(lakhs) + " Lakh" + (remainder !== 0 ? " " + numberToWords(remainder) : "");
      }
      const crores = Math.floor(num / 10000000);
      const remainder = num % 10000000;
      return convertLessThanThousand(crores) + " Crore" + (remainder !== 0 ? " " + numberToWords(remainder) : "");
    }
    
    const amountInWords = numberToWords(Math.floor(netPaid)) + " Rupees Only";
    
    // Add amount in words
    doc
      .fontSize(9)
      .font("Helvetica-Bold")
      .text(`Total Paid (in words): ${amountInWords}`, contentLeft, y, {
        width: usableWidth,
      });
    y += 18;

    const boxWidth2 = 200;
    const noteText =
      "* This receipt is generated by the Madhurekha Eye Care Centre. Disputes if any is subjected to Jamshedpur jurisdiction.";

    const spaceForNote = usableWidth - boxWidth2 - 12;
    const noteWidth = Math.max(120, Math.min(spaceForNote, usableWidth - 12));

    doc
      .fontSize(8)
      .font("Helvetica")
      .text(noteText, contentLeft, y, { width: noteWidth });

    const boxX2 = contentRight - boxWidth2;
    const boxY2 = y;

    const lineH = 14;
    const rowsCount = 3;
    const boxHeight2 = rowsCount * lineH + 8;

    if (boxY2 + boxHeight2 + 60 > doc.page.height) {
      doc.addPage();
      drawPageBorder();
      ({ contentLeft, contentTop, contentRight, usableWidth } =
        computeContentArea());
      y = contentTop;
      drawPageHeader();
      
      // Re-add amount in words on new page
      doc
        .fontSize(9)
        .font("Helvetica-Bold")
        .text(`Total Paid (in words): ${amountInWords}`, contentLeft, y, {
          width: usableWidth,
        });
      y += 18;
      
      doc
        .fontSize(8)
        .font("Helvetica")
        .text(noteText, contentLeft, y, {
          width: Math.max(120, usableWidth - boxWidth2 - 12),
        });
    }

    doc.rect(boxX2, boxY2, boxWidth2, boxHeight2).stroke();
    let by = boxY2 + 6;
    doc.font("Helvetica").fontSize(9);
    const addRow = (label, value) => {
      doc.text(label, boxX2 + 6, by);
      doc.text(`Rs ${formatMoney(value)}`, boxX2 + 6, by, {
        width: boxWidth2 - 12,
        align: "right",
      });
      by += lineH;
    };

    addRow("Total Payable", total);
    addRow("Refund", totalRefunded);
    addRow("Total Paid", netPaid);

    y = Math.max(boxY2 + boxHeight2 + 20, y + 20);

    // ---------- SIGNATURES ----------
    const rightSigX = contentRight - 160;
    doc
      .moveTo(rightSigX, y + 28)
      .lineTo(rightSigX + 160, y + 28)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX, y + 32, {
      width: 160,
      align: "center",
    });

    doc.end();
  } catch (err) {
    console.error("full-payment-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate full payment PDF" });
    } else {
      try {
        res.end();
      } catch (e) {}
    }
  }
});

// DELETE bill + items + payments + refunds + sheet rows
app.delete("/api/bills/:id", async (req, res) => {
  const billId = req.params.id;
  if (!billId) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();
    const invoiceNo = bill.invoiceNo || billId;

    // ---- FETCH CHILD DOCUMENTS ----
    const itemsSnap = await db
      .collection("items")
      .where("billId", "==", billId)
      .get();
    const paymentsSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();
    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", billId)
      .get();

    // ---- BATCH DELETE ----
    const batch = db.batch();

    batch.delete(billRef);

    itemsSnap.forEach((doc) => batch.delete(doc.ref));
    paymentsSnap.forEach((doc) => batch.delete(doc.ref));
    refundsSnap.forEach((doc) => batch.delete(doc.ref));

    await batch.commit();

    // ---- CLEAR CACHE ----
    cache.flushAll();

    // ---- GOOGLE SHEETS DELETE ----
    syncDeleteBillFromSheet(invoiceNo);
    syncDeleteItemsFromSheet(billId);
    syncDeletePaymentsFromSheet(billId);
    syncDeleteRefundsFromSheet(billId);

    return res.json({
      success: true,
      message: "Bill, Items, Payments, Refunds deleted successfully",
      deleted: {
        billId,
        items: itemsSnap.size,
        payments: paymentsSnap.size,
        refunds: refundsSnap.size,
      },
    });
  } catch (err) {
    console.error("DELETE /api/bills/:id error:", err);
    return res.status(500).json({ error: "Failed to delete bill" });
  }
});

// ---------- GET /api/profile (fetch clinic profile) ----------
app.get("/api/profile", async (_req, res) => {
  try {
    const key = makeCacheKey("profile", "clinic");
    const data = await getOrSetCache(key, 120, async () => {
      const profileRef = db.collection("settings").doc("clinicProfile");
      const profileSnap = await profileRef.get();

      if (!profileSnap.exists) {
        // Return null if profile doesn't exist (first time setup)
        return { exists: false };
      }

      return { exists: true, ...profileSnap.data() };
    });

    res.json(data);
  } catch (err) {
    console.error("GET /api/profile error:", err);
    res.status(500).json({ error: "Failed to fetch profile" });
  }
});

// ---------- PUT /api/profile (update clinic profile) ----------
app.put("/api/profile", async (req, res) => {
  try {
    const {
      session,
      clinicName,
      address,
      pan,
      regNo,
      doctor1Name,
      doctor1RegNo,
      doctor2Name,
      doctor2RegNo,
      patientRepresentative,
      clinicRepresentative,
      phone,
      email,
      website,
    } = req.body;

    const profileData = {
      session: session || "",
      clinicName: clinicName || "",
      address: address || "",
      pan: pan || "",
      regNo: regNo || "",
      doctor1Name: doctor1Name || "",
      doctor1RegNo: doctor1RegNo || "",
      doctor2Name: doctor2Name || "",
      doctor2RegNo: doctor2RegNo || "",
      patientRepresentative: patientRepresentative || "",
      clinicRepresentative: clinicRepresentative || "",
      phone: phone || "",
      email: email || "",
      website: website || "",
      updatedAt: new Date().toISOString(),
    };

    const profileRef = db.collection("settings").doc("clinicProfile");
    await profileRef.set(profileData, { merge: true });

    // Clear cache
    cache.flushAll();

    // Sync to Google Sheet (optional - add this function to sheetIntregation.js)
    try {
      syncProfileToSheet(profileData);
    } catch (sheetErr) {
      console.warn("Sheet sync failed for profile:", sheetErr);
    }

    res.json({
      success: true,
      message: "Profile updated successfully",
      profile: profileData,
    });
  } catch (err) {
    console.error("PUT /api/profile error:", err);
    res.status(500).json({ error: "Failed to update profile" });
  }
});

// (Aapka PUT /api/refunds/:id already hai line ~1227 pe, so ye skip karo)
app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();

    // 1) LEGACY ITEMS
    const itemsSnap = await db
      .collection("items")
      .where("billId", "==", id)
      .get();

    const legacyItems = itemsSnap.docs.map((doc) => {
      const d = doc.data();
      const qty = Number(d.qty || 0);
      const rate = Number(d.rate || 0);
      const amount = d.amount != null ? Number(d.amount) : qty * rate;

      const description = d.description || d.item || d.details || "";

      return {
        id: doc.id,
        qty,
        rate,
        amount,
        description,
      };
    });

    // 2) NEW SERVICES
    const serviceItems = Array.isArray(bill.services)
      ? bill.services.map((s, idx) => {
          const qty = Number(s.qty || 0);
          const rate = Number(s.rate || 0);
          const amount = s.amount != null ? Number(s.amount) : qty * rate;

          const parts = [];
          if (s.item) parts.push(s.item);
          if (s.details) parts.push(s.details);

          return {
            id: `svc-${idx + 1}`,
            qty,
            rate,
            amount,
            description: parts.join(" - "),
          };
        })
      : [];

    // 3) FINAL ITEMS (services take precedence)
    const items = serviceItems.length > 0 ? serviceItems : legacyItems;

    // 4) PAYMENTS
    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", id)
      .get();

    const payments = paysSnap.docs
      .map((doc) => {
        const d = doc.data();
        const paymentDateTime =
          d.paymentDateTime ||
          (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
        return {
          id: doc.id,
          paymentDateTime,
          amount: Number(d.amount || 0),
          mode: d.mode || null,
          referenceNo: d.referenceNo || null,
          drawnOn: d.drawnOn || null,
          drawnAs: d.drawnAs || null,
        };
      })
      // sort by date ASC so earliest (first) payment is first in array
      .sort((a, b) => {
        const da = a.paymentDateTime
          ? new Date(a.paymentDateTime)
          : new Date(0);
        const dbb = b.paymentDateTime
          ? new Date(b.paymentDateTime)
          : new Date(0);
        return da - dbb;
      });

    // primaryPayment = first (earliest) payment (explicit)
    const primaryPayment = payments.length > 0 ? payments[0] : null;

    const totalPaidGross = payments.reduce(
      (sum, p) => sum + Number(p.amount || 0),
      0
    );

    // 5) REFUNDS (still used for math but not printed as a separate line)
    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", id)
      .get();

    const refunds = refundsSnap.docs.map((doc) => {
      const d = doc.data();
      return Number(d.amount || 0);
    });

    const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

    // 6) TOTALS
    let total = Number(bill.total || 0);

    // if bill.total is not present, compute from items (so adding items updates invoice)
    if (!total && items.length > 0) {
      total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
    }

    const paidNet = totalPaidGross - totalRefunded;
    const balance = total - paidNet;

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    // ensure invoice no is fixed once generated: if missing, generate and save back
    const generatedInvoiceNo = `S-${id}`;
    const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
    if (!bill.invoiceNo) {
      // best-effort: persist invoice number so next time invoice remains same
      try {
        await billRef.update({ invoiceNo });
      } catch (e) {
        // non-fatal: continue even if update fails (e.g., permission)
        console.warn("Failed to persist invoiceNo:", e);
      }
    }

    const dateText =
      typeof formatDateDot === "function"
        ? formatDateDot(bill.date || "")
        : bill.date || "";
    const patientName = bill.patientName || "";
    const ageText =
      bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
    const sexText = bill.sex ? String(bill.sex) : "";

    const paymentMode = primaryPayment?.mode || "Cash";
    const referenceNo = primaryPayment?.referenceNo || null;
    const drawnOn = primaryPayment?.drawnOn || null;
    const drawnAs = primaryPayment?.drawnAs || null;

    const drawnOnText = drawnOn || "________________________";
    const drawnAsText = drawnAs || "________________________";

    // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
    const profile = await getClinicProfile({ force: true });
    const clinicName = profileValue(profile, "clinicName");
    const clinicAddress = profileValue(profile, "address");
    const clinicPAN = profileValue(profile, "pan");
    const clinicRegNo = profileValue(profile, "regNo");
    const doctor1Name = profileValue(profile, "doctor1Name");
    const doctor1RegNo = profileValue(profile, "doctor1RegNo");
    const doctor2Name = profileValue(profile, "doctor2Name");
    const doctor2RegNo = profileValue(profile, "doctor2RegNo");
    const patientRepresentative = profileValue(
      profile,
      "patientRepresentative"
    );
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

    // 7) PDF START
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="invoice-${id}.pdf"`
    );

    const doc = new PDFDocument({
      size: "A4",
      margin: 36,
    });

    doc.pipe(res);

    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72;
    let y = 36;

    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

    const leftX = 36;
    const rightX = pageWidth - 36;

    try {
      doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
    } catch (e) {}

    try {
      doc.image(logoRightPath, rightX - 45, y, {
        width: 45,
        height: 45,
      });
    } catch (e) {}

    // CLINIC HEADER (from profile)
    doc
      .font("Helvetica-Bold")
      .fontSize(16)
      .text(clinicName || "", 0, y + 4, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(clinicAddress || "", 0, y + 24, {
        align: "center",
        width: pageWidth,
      })
      .text(
        (clinicPAN || "") +
          (clinicPAN || clinicRegNo ? "   |   " : "") +
          (clinicRegNo || ""),
        {
          align: "center",
          width: pageWidth,
        }
      );

    y += 60;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();

    y += 4;

    // static doctor names replaced with profile values
    doc.fontSize(9).font("Helvetica-Bold");
    doc.text(doctor1Name || "", 36, y);
    doc.text(doctor2Name || "", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
    doc.text(
      doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );

    y += 18;

    // invoice title bar
    doc.rect(36, y, usableWidth, 18).stroke();
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
        align: "center",
        width: usableWidth,
      });

    y += 26;

    doc.font("Helvetica").fontSize(9);

    // Invoice + Date row
    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

    // Mr/Mrs + Age row
    doc.text(`Mr./Mrs.: ${patientName}`, 36, y, {
      width: usableWidth * 0.6,
    });
    if (ageText) {
      doc.text(`Age: ${ageText}`, pageWidth / 2, y, {
        align: "right",
        width: usableWidth / 2,
      });
    }

    y += 12;

    // Address + Sex row (sex nayi line pe, address ke saath)
    doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
      width: usableWidth * 0.6,
    });
    if (sexText) {
      doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
        align: "right",
        width: usableWidth / 2,
      });
    }

    y += 20;

    // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
    const tableLeft = 36;
    const colSrW = 22;
    const colQtyW = 48;
    const colRateW = 70;
    const colSubW = 70; // amount
    const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colSubW);

    const colSrX = tableLeft;
    const colServiceX = colSrX + colSrW;
    const colQtyX = colServiceX + colServiceW;
    const colRateX = colQtyX + colQtyW;
    const colSubX = colRateX + colRateW;
    const tableRightX = tableLeft + usableWidth;

    // layout constants
    const headerHeight = 16;
    const rowHeight = 14;
    const minTableHeight = 200; // change this value to adjust fixed visual table height
    const bottomSafety = 120; // reserved area to avoid overlapping footer

    // We'll draw only one horizontal line after the header.
    // We will NOT draw horizontal borders per row.
    // Vertical separators will be drawn for the whole content block per page segment.

    // helper to draw vertical separators for a vertical segment (on current page)
    function drawVerticalsForSegment(yTop, height) {
      const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
      const top = yTop;
      const bottom = yTop + height;
      xs.forEach((x) => {
        doc.moveTo(x, top).lineTo(x, bottom).stroke();
      });
    }

    // remember table start y so we can enforce min height later
    const tableStartY = y;

    // header background + border
    const headerTopY = y;
    doc
      .save()
      .rect(tableLeft, headerTopY, usableWidth, headerHeight)
      .fill("#F3F3F3")
      .restore()
      .rect(tableLeft, headerTopY, usableWidth, headerHeight)
      .stroke();

    // draw vertical separators for header (so headings have same vertical lines as body)
    drawVerticalsForSegment(headerTopY, headerHeight);

    // header text: add top padding by using y + 6
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Sr.", colSrX + 2, headerTopY + 6);
    doc.text(
      "Description of Items / Services",
      colServiceX + 2,
      headerTopY + 6,
      {
        width: colServiceW - 4,
      }
    );
    doc.text("Qty", colQtyX + 2, headerTopY + 6);
    doc.text("Rate", colRateX + 2, headerTopY + 6, {
      width: colRateW - 4,
      align: "right",
    });
    doc.text("Amount", colSubX + 2, headerTopY + 6, {
      width: colSubW - 4,
      align: "right",
    });

    y += headerHeight;

    // draw a single horizontal separator line just below the header (one horizontal row after heading)
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // We will accumulate a single vertical segment per page.
    let segmentStartY = y; // start of content area for current page
    let segmentHeight = 0; // how tall content area grows on current page

    doc.font("Helvetica").fontSize(9);

    // iterate items and draw content (no horizontal borders). Manage page breaks and vertical segments.
    for (let idx = 0; idx < items.length; idx++) {
      const item = items[idx];

      // if not enough space for one row + bottomSafety, finish current vertical segment, draw verticals,
      // then create new page and redraw header and header separator.
      if (y + rowHeight > doc.page.height - bottomSafety) {
        // draw vertical separators for the segment we just filled on THIS page
        if (segmentHeight > 0) {
          drawVerticalsForSegment(segmentStartY, segmentHeight);
        }

        doc.addPage();
        y = 36;

        // redraw small page header area (logos not necessary) — keep consistent header rendering
        try {
          doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
        } catch (e) {}
        try {
          doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
        } catch (e) {}
        // clinic name
        doc
          .font("Helvetica-Bold")
          .fontSize(16)
          .text(clinicName || "", 0, y + 4, {
            align: "center",
            width: pageWidth,
          });
        doc
          .font("Helvetica")
          .fontSize(9)
          .text(clinicAddress || "", 0, y + 24, {
            align: "center",
            width: pageWidth,
          });
        y += 60;
        // small dividing line
        doc
          .moveTo(36, y)
          .lineTo(pageWidth - 36, y)
          .stroke();
        y += 10;

        // redraw table header on new page
        const headerTopY2 = y;
        // header background + border
        doc
          .save()
          .rect(tableLeft, headerTopY2, usableWidth, headerHeight)
          .fill("#F3F3F3")
          .restore()
          .rect(tableLeft, headerTopY2, usableWidth, headerHeight)
          .stroke();

        // draw vertical separators for the header on the new page
        drawVerticalsForSegment(headerTopY2, headerHeight);

        // header text on new page with top padding (y + 6)
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Sr.", colSrX + 2, headerTopY2 + 6);
        doc.text(
          "Description of Items / Services",
          colServiceX + 2,
          headerTopY2 + 6,
          { width: colServiceW - 4 }
        );
        doc.text("Qty", colQtyX + 2, headerTopY2 + 6);
        doc.text("Rate", colRateX + 2, headerTopY2 + 6, {
          width: colRateW - 4,
          align: "right",
        });
        doc.text("Amount", colSubX + 2, headerTopY2 + 6, {
          width: colSubW - 4,
          align: "right",
        });
        y += headerHeight;

        // draw the single horizontal separator again under header
        doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

        // reset segment tracking for the new page
        segmentStartY = y;
        segmentHeight = 0;

        doc.font("Helvetica").fontSize(9);
      }

      // draw row content (no horizontal box) with top padding for text (y + 6)
      doc.text(String(idx + 1), colSrX + 2, y + 6);
      doc.text(item.description || "", colServiceX + 2, y + 6, {
        width: colServiceW - 4,
      });
      doc.text(
        item.qty != null && item.qty !== "" ? String(item.qty) : "",
        colQtyX + 2,
        y + 6
      );
      doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 6, {
        width: colRateW - 4,
        align: "right",
      });
      doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 6, {
        width: colSubW - 4,
        align: "right",
      });

      // advance y and increment segmentHeight
      y += rowHeight;
      segmentHeight += rowHeight;
    }

    // After drawing items, we still want the table to have a minimum visual height.
    // Draw empty rows (content only) to reach minTableHeight — but don't draw horizontal lines.
    const currentTableHeight = y - tableStartY;
    if (currentTableHeight < minTableHeight) {
      let remainingHeight = minTableHeight - currentTableHeight;
      const emptyRows = Math.ceil(remainingHeight / rowHeight);
      for (let i = 0; i < emptyRows; i++) {
        // handle page break while adding empty rows
        if (y + rowHeight > doc.page.height - bottomSafety) {
          // draw verticals for this page before page break
          if (segmentHeight > 0) {
            drawVerticalsForSegment(segmentStartY, segmentHeight);
          }

          doc.addPage();
          y = 36;

          // redraw condensed header area and table header on new page
          try {
            doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
          } catch (e) {}
          try {
            doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
          } catch (e) {}
          doc
            .font("Helvetica-Bold")
            .fontSize(16)
            .text(clinicName || "", 0, y + 4, {
              align: "center",
              width: pageWidth,
            });
          doc
            .font("Helvetica")
            .fontSize(9)
            .text(clinicAddress || "", 0, y + 24, {
              align: "center",
              width: pageWidth,
            });
          y += 60;
          doc
            .moveTo(36, y)
            .lineTo(pageWidth - 36, y)
            .stroke();
          y += 10;

          // redraw table header
          const headerTopY3 = y;
          doc
            .save()
            .rect(tableLeft, headerTopY3, usableWidth, headerHeight)
            .fill("#F3F3F3")
            .restore()
            .rect(tableLeft, headerTopY3, usableWidth, headerHeight)
            .stroke();

          // draw vertical separators for the header for this new page (filler case)
          drawVerticalsForSegment(headerTopY3, headerHeight);

          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Sr.", colSrX + 2, headerTopY3 + 6);
          doc.text(
            "Description of Items / Services",
            colServiceX + 2,
            headerTopY3 + 6,
            { width: colServiceW - 4 }
          );
          doc.text("Qty", colQtyX + 2, headerTopY3 + 6);
          doc.text("Rate", colRateX + 2, headerTopY3 + 6, {
            width: colRateW - 4,
            align: "right",
          });
          doc.text("Amount", colSubX + 2, headerTopY3 + 6, {
            width: colSubW - 4,
            align: "right",
          });
          y += headerHeight;

          // single horizontal separator under header
          doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

          // reset segment tracking
          segmentStartY = y;
          segmentHeight = 0;

          doc.font("Helvetica").fontSize(9);
        }

        // advance y for an empty visual row (no horizontal border)
        y += rowHeight;
        segmentHeight += rowHeight;
      }
    }

    // finally, draw vertical separators for the last page's segment
    if (segmentHeight > 0) {
      drawVerticalsForSegment(segmentStartY, segmentHeight);
    }

    // --- NEW: draw a single horizontal separator line AFTER the last content row (immediately above totals)
    // Draw at current y (this is right after last row / filler).
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // Note: do NOT add extra gap here — totals should start immediately (margin 0).
    // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
    const boxWidth = 180;
    const boxX = pageWidth - 36 - boxWidth;
    // place totals box with its top exactly at current y (no margin)
    const boxY = y;
    const lineH = 18; // make totals box slightly taller to give top padding inside

    // If there isn't enough vertical space to draw totals box at this position, shift to new page.
    if (boxY + lineH + 60 > doc.page.height) {
      doc.addPage();
      y = 36;
    }

    // draw totals box and border
    doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

    doc.fontSize(9).font("Helvetica");

    // Give top padding inside totals box by using boxY + 6
    doc.font("Helvetica-Bold");
    doc.text("Total", boxX + 6, boxY + 6);
    doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 6, {
      width: boxWidth - 6,
      align: "right",
    });

    // set y just below totals box for the remaining content
    y = boxY + lineH + 20;

    // NET PAID + BALANCE (these remain)
    doc.font("Helvetica").fontSize(9);
    doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
    doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 18;

    // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Payment Details:", 36, y);
    y += 12;

    doc.font("Helvetica").fontSize(8);
    doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
      width: usableWidth / 2,
    });
    doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
      width: usableWidth / 2,
      align: "right",
    });
    y += 12;

    doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
      width: usableWidth / 2,
    });
    doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
      width: usableWidth / 2,
      align: "right",
    });

    y += 20;

    // FOOTER NOTES
    doc
      .fontSize(8)
      .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
        width: usableWidth,
      });

    y = doc.y + 30;

    // SIGNATURE LINES
    const sigWidth = 160;
    const sigY = y;

    doc
      .moveTo(36, sigY)
      .lineTo(36 + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    const rightSigX2 = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX2, sigY)
      .lineTo(rightSigX2 + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX2, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    doc.end();
  } catch (err) {
    console.error("invoice-html-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate invoice PDF" });
    }
  }
});

// ---------- PDF: Bill Summary (A4 full page, chronological timeline) ----------
app.get("/api/bills/:id/summary-pdf", async (req, res) => {
  const billId = req.params.id;
  if (!billId) return res.status(400).json({ error: "Invalid bill id" });
  try {
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();
    const billTotal = Number(bill.total || 0);
    // --- PAYMENTS ---
    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();
    const payments = paysSnap.docs.map((doc) => {
      const d = doc.data();
      const paymentDateTime = d.paymentDate
        ? `${d.paymentDate}T00:00:00.000Z`
        : d.paymentDateTime || null;

      return {
        id: doc.id,
        amount: Number(d.amount || 0),
        paymentDateTime,
        mode: d.mode || "",
        referenceNo: d.referenceNo || null,
        receiptNo: d.receiptNo || null,
        chequeNo: d.chequeNo || null, // <-- yeh line
      };
    });

    payments.sort((a, b) => {
      const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
      const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
      return da - dbb;
    });
    const totalPaidGross = payments.reduce(
      (sum, p) => sum + Number(p.amount || 0),
      0
    );
    // --- REFUNDS ---
    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", billId)
      .get();
    const refunds = refundsSnap.docs.map((doc) => {
      const d = doc.data();
      const refundDateTime = d.refundDate
        ? `${d.refundDate}T00:00:00.000Z`
        : d.refundDateTime || null;
      return {
        id: doc.id,
        amount: Number(d.amount || 0),
        refundDateTime,
        mode: d.mode || "",
        referenceNo: d.referenceNo || null,
        refundNo: d.refundReceiptNo || null,
      };
    });
    refunds.sort((a, b) => {
      const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
      const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
      return da - dbb;
    });
    const totalRefunded = refunds.reduce(
      (sum, r) => sum + Number(r.amount || 0),
      0
    );
    const netPaid = totalPaidGross - totalRefunded;
    const balance = billTotal - netPaid;
    const paymentsCount = payments.length;
    const refundsCount = refunds.length;
    const patientName = bill.patientName || "";
    const address = bill.address || "";
    const invoiceNo = bill.invoiceNo || billId;
    const billDate = bill.date || "";
    const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");
    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }
    function formatDateTime(dtString) {
      if (!dtString) return "";
      return typeof formatDateTimeDot === "function"
        ? formatDateTimeDot(dtString)
        : dtString;
    }
    function formatDateOnly(dtString) {
      if (!dtString) return "";
      return typeof formatDateDot === "function"
        ? formatDateDot(dtString)
        : String(dtString).split("T")[0];
    }
    // --------- BUILD CHRONOLOGICAL TIMELINE ---------
    const timeline = [];
    const invoiceDateTime =
      bill.createdAt || (bill.date ? `${bill.date}T00:00:00.000Z` : null);
    timeline.push({
      type: "INVOICE",
      label: "Invoice Generated",
      dateTime: invoiceDateTime,
      mode: "-",
      ref: invoiceNo,
      debit: billTotal,
      credit: 0,
    });
    payments.forEach((p) => {
      const isCheque = (p.mode || "").toLowerCase() === "cheque";

      timeline.push({
        type: "PAYMENT",
        label: p.receiptNo ? `Payment Receipt (${p.receiptNo})` : "Payment",
        dateTime: p.paymentDateTime,
        mode: p.mode || "",
        ref: isCheque && p.chequeNo ? p.chequeNo : p.referenceNo || "",
        debit: 0,
        credit: p.amount,
      });
    });

    refunds.forEach((r) => {
      timeline.push({
        type: "REFUND",
        label: r.refundNo ? `Refund (${r.refundNo})` : "Refund",
        dateTime: r.refundDateTime,
        mode: r.mode || "",
        ref: r.referenceNo || "",
        debit: r.amount,
        credit: 0,
      });
    });
    timeline.sort((a, b) => {
      const da = a.dateTime ? new Date(a.dateTime) : new Date(0);
      const dbb = b.dateTime ? new Date(b.dateTime) : new Date(0);
      return da - dbb;
    });
    // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
    const profile = await getClinicProfile({ force: true });
    const clinicName = profileValue(profile, "clinicName");
    const clinicAddress = profileValue(profile, "address");
    const clinicPAN = profileValue(profile, "pan");
    const clinicRegNo = profileValue(profile, "regNo");
    const doctor1Name = profileValue(profile, "doctor1Name");
    const doctor1RegNo = profileValue(profile, "doctor1RegNo");
    const doctor2Name = profileValue(profile, "doctor2Name");
    const doctor2RegNo = profileValue(profile, "doctor2RegNo");
    const patientRepresentative = profileValue(
      profile,
      "patientRepresentative"
    );
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");
    // ---------- PDF START ----------
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="bill-summary-${billId}.pdf"`
    );
    const doc = new PDFDocument({
      size: "A4",
      margin: 36,
    });
    doc.pipe(res);
    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72;
    let y = 40;
    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
    const leftX = 36;
    const rightX = pageWidth - 36;
    try {
      doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
    } catch (e) {}
    try {
      doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
    } catch (e) {}
    doc
      .font("Helvetica-Bold")
      .fontSize(13)
      .text(clinicName || "", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });
    doc
      .font("Helvetica")
      .fontSize(9)
      .text(clinicAddress || "", 0, y + 20, {
        align: "center",
        width: pageWidth,
      })
      .text(
        (clinicPAN || "") +
          (clinicPAN || clinicRegNo ? " | " : "") +
          (clinicRegNo || ""),
        {
          align: "center",
          width: pageWidth,
        }
      );
    y += 48;
    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;
    // static doctor header
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text(doctor1Name || "", 36, y);
    doc.text(doctor2Name || "", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });
    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
    doc.text(
      doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "",
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );
    y += 16;
    // title bar
    doc
      .save()
      .rect(36, y, usableWidth, 16)
      .fill("#F3F3F3")
      .restore()
      .rect(36, y, usableWidth, 16)
      .stroke();
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("BILL SUMMARY", 36, y + 3, {
        align: "center",
        width: usableWidth,
      });
    y += 24;
    // invoice / patient line
    doc.font("Helvetica").fontSize(9);
    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(
      `Date: ${
        typeof formatDateDot === "function" ? formatDateDot(billDate) : billDate
      }`,
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );
    y += 12;
    doc.text(`Patient Name: ${patientName}`, 36, y, {
      width: usableWidth,
    });
    doc.font("Helvetica").text(`Address: ${address}`, 36, doc.y + 3, {
      width: usableWidth,
    });
    y += 28;
    // --------- CHRONOLOGICAL TABLE ---------
    const tableLeft = 36;
    const colDateW = 60;
    const colPartW = 160;
    const colModeW = 60;
    const colRefW = 80;
    const colDebitW = 50;
    const colCreditW = 50;
    const colBalW =
      usableWidth -
      (colDateW + colPartW + colModeW + colRefW + colDebitW + colCreditW);
    const colDateX = tableLeft;
    const colPartX = colDateX + colDateW;
    const colModeX = colPartX + colPartW;
    const colRefX = colModeX + colModeW;
    const colDebitX = colRefX + colRefW;
    const colCreditX = colDebitX + colDebitW;
    const colBalX = colCreditX + colCreditW;
    const tableRightX = tableLeft + usableWidth;
    const headerHeight = 18;
    const minRowHeight = 14;
    const minTableHeight = 200;
    const bottomSafety = 120;

    function drawVerticalsForSegment(yTop, height) {
      const xs = [
        colDateX,
        colPartX,
        colModeX,
        colRefX,
        colDebitX,
        colCreditX,
        colBalX,
        tableRightX,
      ];
      const top = yTop;
      const bottom = yTop + height;
      xs.forEach((x) => {
        doc.moveTo(x, top).lineTo(x, bottom).stroke();
      });
    }

    const tableStartY = y;
    const headerTopY = y;
    doc
      .save()
      .rect(tableLeft, headerTopY, usableWidth, headerHeight)
      .fill("#F3F3F3")
      .restore()
      .rect(tableLeft, headerTopY, usableWidth, headerHeight)
      .stroke();
    drawVerticalsForSegment(headerTopY, headerHeight);
    doc.font("Helvetica-Bold").fontSize(8);
    doc.text("Date", colDateX + 4, headerTopY + 5, {
      width: colDateW - 6,
    });
    doc.text("Particulars", colPartX + 4, headerTopY + 5, {
      width: colPartW - 6,
    });
    doc.text("Mode", colModeX + 4, headerTopY + 5, {
      width: colModeW - 6,
    });
    doc.text("Reference", colRefX + 4, headerTopY + 5, {
      width: colRefW - 6,
    });
    doc.text("Debit (Rs)", colDebitX + 4, headerTopY + 5, {
      width: colDebitW - 6,
      align: "right",
    });
    doc.text("Credit (Rs)", colCreditX + 4, headerTopY + 5, {
      width: colCreditW - 6,
      align: "right",
    });
    doc.text("Balance (Rs)", colBalX + 4, headerTopY + 5, {
      width: colBalW - 6,
      align: "right",
    });
    y += headerHeight;
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    let segmentStartY = y;
    let segmentHeight = 0;
    doc.font("Helvetica").fontSize(8);
    let runningBalance = 0;

    for (let i = 0; i < timeline.length; i++) {
      const ev = timeline[i];
      const dateStr = formatDateOnly(ev.dateTime);
      const partStr = ev.label || "";
      const modeStr = ev.mode || "";
      const refStr = ev.ref || "";
      const debitStr = ev.debit ? formatMoney(ev.debit) : "";
      const creditStr = ev.credit ? formatMoney(ev.credit) : "";

      const padding = 6;
      const dateH = doc.heightOfString(String(dateStr), {
        width: colDateW - 8,
      });
      const partH = doc.heightOfString(String(partStr), {
        width: colPartW - 8,
      });
      const modeH = doc.heightOfString(String(modeStr), {
        width: colModeW - 8,
      });
      const refH = doc.heightOfString(String(refStr), { width: colRefW - 8 });
      const debitH = doc.heightOfString(String(debitStr), {
        width: colDebitW - 8,
      });
      const creditH = doc.heightOfString(String(creditStr), {
        width: colCreditW - 8,
      });
      const balH = doc.heightOfString(
        String(
          formatMoney(
            ev.type === "INVOICE"
              ? ev.debit - ev.credit
              : runningBalance + ev.debit - ev.credit
          )
        ),
        { width: colBalW - 8 }
      );
      const contentMaxH = Math.max(
        dateH,
        partH,
        modeH,
        refH,
        debitH,
        creditH,
        balH
      );
      let rowH = Math.max(minRowHeight, contentMaxH + padding);

      if (y + rowH > doc.page.height - bottomSafety) {
        if (segmentHeight > 0) {
          drawVerticalsForSegment(segmentStartY, segmentHeight);
        }
        doc.addPage();
        y = 36;
        try {
          doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
        } catch (e) {}
        try {
          doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
        } catch (e) {}
        doc
          .font("Helvetica-Bold")
          .fontSize(13)
          .text(clinicName || "", 0, y + 2, {
            align: "center",
            width: pageWidth,
          });
        doc
          .font("Helvetica")
          .fontSize(9)
          .text(clinicAddress || "", 0, y + 20, {
            align: "center",
            width: pageWidth,
          });
        y += 48;
        doc
          .moveTo(36, y)
          .lineTo(pageWidth - 36, y)
          .stroke();
        y += 6;
        const headerTopY2 = y;
        doc
          .save()
          .rect(tableLeft, headerTopY2, usableWidth, headerHeight)
          .fill("#F3F3F3")
          .restore()
          .rect(tableLeft, headerTopY2, usableWidth, headerHeight)
          .stroke();
        drawVerticalsForSegment(headerTopY2, headerHeight);
        doc.font("Helvetica-Bold").fontSize(8);
        doc.text("Date & Time", colDateX + 4, headerTopY2 + 5, {
          width: colDateW - 6,
        });
        doc.text("Particulars", colPartX + 4, headerTopY2 + 5, {
          width: colPartW - 6,
        });
        doc.text("Mode", colModeX + 4, headerTopY2 + 5, {
          width: colModeW - 6,
        });
        doc.text("Reference", colRefX + 4, headerTopY2 + 5, {
          width: colRefW - 6,
        });
        doc.text("Debit (Rs)", colDebitX + 4, headerTopY2 + 5, {
          width: colDebitW - 6,
          align: "right",
        });
        doc.text("Credit (Rs)", colCreditX + 4, headerTopY2 + 5, {
          width: colCreditW - 6,
          align: "right",
        });
        doc.text("Balance (Rs)", colBalX + 4, headerTopY2 + 5, {
          width: colBalW - 6,
          align: "right",
        });
        y += headerHeight;
        doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();
        segmentStartY = y;
        segmentHeight = 0;
        doc.font("Helvetica").fontSize(8);
      }

      if (ev.type === "INVOICE") {
        runningBalance = ev.debit - ev.credit;
      } else {
        runningBalance += ev.debit;
        runningBalance -= ev.credit;
      }

      const cellTop = y + 3;
      doc.text(dateStr, colDateX + 4, cellTop, { width: colDateW - 8 });
      doc.text(partStr, colPartX + 4, cellTop, { width: colPartW - 8 });
      doc.text(modeStr, colModeX + 4, cellTop, { width: colModeW - 8 });
      doc.text(refStr, colRefX + 4, cellTop, { width: colRefW - 8 });
      doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 4, cellTop, {
        width: colDebitW - 8,
        align: "right",
      });
      doc.text(
        ev.credit ? formatMoney(ev.credit) : "",
        colCreditX + 4,
        cellTop,
        { width: colCreditW - 8, align: "right" }
      );
      doc.text(formatMoney(runningBalance), colBalX + 4, cellTop, {
        width: colBalW - 8,
        align: "right",
      });

      y += rowH;
      segmentHeight += rowH;
    }

    // Fill minimum table height with empty rows
    const currentTableHeight = y - tableStartY;
    if (currentTableHeight < minTableHeight) {
      let remainingHeight = minTableHeight - currentTableHeight;
      const emptyRows = Math.ceil(remainingHeight / minRowHeight);
      for (let i = 0; i < emptyRows; i++) {
        if (y + minRowHeight > doc.page.height - bottomSafety) {
          if (segmentHeight > 0) {
            drawVerticalsForSegment(segmentStartY, segmentHeight);
          }
          doc.addPage();
          y = 36;
          try {
            doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
          } catch (e) {}
          try {
            doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
          } catch (e) {}
          doc
            .font("Helvetica-Bold")
            .fontSize(13)
            .text(clinicName || "", 0, y + 2, {
              align: "center",
              width: pageWidth,
            });
          doc
            .font("Helvetica")
            .fontSize(9)
            .text(clinicAddress || "", 0, y + 20, {
              align: "center",
              width: pageWidth,
            });
          y += 48;
          doc
            .moveTo(36, y)
            .lineTo(pageWidth - 36, y)
            .stroke();
          y += 6;
          const headerTopY3 = y;
          doc
            .save()
            .rect(tableLeft, headerTopY3, usableWidth, headerHeight)
            .fill("#F3F3F3")
            .restore()
            .rect(tableLeft, headerTopY3, usableWidth, headerHeight)
            .stroke();
          drawVerticalsForSegment(headerTopY3, headerHeight);
          doc.font("Helvetica-Bold").fontSize(8);
          doc.text("Date & Time", colDateX + 4, headerTopY3 + 5, {
            width: colDateW - 6,
          });
          doc.text("Particulars", colPartX + 4, headerTopY3 + 5, {
            width: colPartW - 6,
          });
          doc.text("Mode", colModeX + 4, headerTopY3 + 5, {
            width: colModeW - 6,
          });
          doc.text("Reference", colRefX + 4, headerTopY3 + 5, {
            width: colRefW - 6,
          });
          doc.text("Debit (Rs)", colDebitX + 4, headerTopY3 + 5, {
            width: colDebitW - 6,
            align: "right",
          });
          doc.text("Credit (Rs)", colCreditX + 4, headerTopY3 + 5, {
            width: colCreditW - 6,
            align: "right",
          });
          doc.text("Balance (Rs)", colBalX + 4, headerTopY3 + 5, {
            width: colBalW - 6,
            align: "right",
          });
          y += headerHeight;
          doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();
          segmentStartY = y;
          segmentHeight = 0;
          doc.font("Helvetica").fontSize(8);
        }
        y += minRowHeight;
        segmentHeight += minRowHeight;
      }
    }

    if (segmentHeight > 0) {
      drawVerticalsForSegment(segmentStartY, segmentHeight);
    }
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // ============ NOTE ON LEFT (as requested) ============
    doc
      .fontSize(8)
      .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y + 10, {
        width: usableWidth / 2, // restrict to left half
        align: "left",
      });

    // ============ TOTALS BOX ON RIGHT ============
    const boxWidth = 220;
    const boxX = pageWidth - 36 - boxWidth;
    const boxY = y + 8; // slight offset from table bottom line
    const lineH2 = 16;
    const rows2 = 6;
    const boxHeight = lineH2 * rows2 + 8;

    if (boxY + boxHeight + 80 > doc.page.height) {
      doc.addPage();
      y = 36;
    } else {
      y = boxY; // update y only if staying on same page
    }

    doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();
    let by3 = boxY + 6;
    doc.font("Helvetica").fontSize(9);
    function boxRow(label, value) {
      doc
        .font("Helvetica")
        .fontSize(9)
        .text(label, boxX + 8, by3);
      doc.text(value, boxX + 8, by3, { width: boxWidth - 16, align: "right" });
      by3 += lineH2;
    }
    boxRow("Bill Total", `Rs ${formatMoney(billTotal)}`);
    boxRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
    boxRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
    boxRow("Net Paid", `Rs ${formatMoney(netPaid)}`);
    boxRow("Balance", `Rs ${formatMoney(balance)}`);
    boxRow("Status", status);

    // ============ SIGNATURE (only clinic on right) ============
    y = Math.max(y, boxY + boxHeight) + 30;
    const sigWidth = 160;
    const sigY = y;
    const rightSigX2 = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX2, sigY)
      .lineTo(rightSigX2 + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX2, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    doc.end();
  } catch (err) {
    console.error("summary-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate summary PDF" });
    }
  }
});



// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
