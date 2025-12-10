// server.js
import express from "express";
import cors from "cors";
import "dotenv/config";
import PDFDocument from "pdfkit";
import path from "path";
import { fileURLToPath } from "url";
import { db } from "./firebaseClient.js";
// at top of server.js
import {
  syncBillToSheet,
  syncItemsToSheet,
  syncPaymentToSheet,
  syncRefundToSheet,
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

// --------- ID HELPERS (FINANCIAL YEAR + SEQUENCES) ----------

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

// Get next sequence number from Firestore counters doc, safely
async function getNextSequence(key) {
  const ref = db.collection("counters").doc(key);
  const nextSeq = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const current = snap.exists ? Number(snap.data().seq || 0) : 0;
    const updated = current + 1;
    tx.set(ref, { seq: updated }, { merge: true });
    return updated;
  });
  return nextSeq;
}

// Generate invoice number: "25-26/INV-0001"
async function generateInvoiceNumber(billDateInput) {
  const dateStr = billDateInput || new Date().toISOString().slice(0, 10);
  const fy = getFinancialYearCode(dateStr);
  const seq = await getNextSequence(`invoice-${fy}`);
  const serial = String(seq).padStart(4, "0");
  const invoiceNo = `${fy}/INV-${serial}`; // e.g. "25-26/INV-0001"

  return { invoiceNo, fy, serial };
}

// Parse "25-26/INV-0001" into { fy: "25-26", invoiceSerial: "0001" }
function parseInvoiceNumber(invoiceNo) {
  // expecting "25-26/INV-0001"
  const [fy, rest] = (invoiceNo || "").split("/");
  if (!fy || !rest) {
    return { fy: "00-00", invoiceSerial: "0000" };
  }
  const parts = rest.split("-");
  const invoiceSerial = parts[1] || "0000";
  return { fy, invoiceSerial };
}

// Generate receipt id: "25-26/INV-0001/Rec-0001"
async function generateReceiptId(invoiceNo) {
  const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);
  const seq = await getNextSequence(`receipt-${fy}-${invoiceSerial}`);
  const recSerial = String(seq).padStart(4, "0");
  return `${fy}/INV-${invoiceSerial}/REC-${recSerial}`;
}

// Generate refund id: "25-26/INV-0001/REF-0001"
async function generateRefundId(invoiceNo) {
  const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);
  const seq = await getNextSequence(`refund-${fy}-${invoiceSerial}`);
  const refSerial = String(seq).padStart(4, "0");
  return `${fy}/INV-${invoiceSerial}/REF-${refSerial}`;
}

// FRONTEND base URL (React app)
const FRONTEND_BASE =
  process.env.FRONTEND_BASE_URL ||
  (process.env.NODE_ENV === "production"
    ? "https://madhu-rekha-billing-software.vercel.app"
    : "http://localhost:5173");

// ---------- HEALTH CHECK ----------
app.get("/", (_req, res) => {
  res.send("Backend OK");
});

//
// FIRESTORE SCHEMA:
//
// bills:
//   { patientName, sex, address, age, date, invoiceNo, subtotal, adjust,
//     total, paid, refunded, balance, doctorReg1, doctorReg2,
//     status, createdAt, remarks, services: [{ item, details, qty, rate, amount }] }
//
// items:
//   { billId, description, qty, rate, amount }
//
// payments:
//   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
//     chequeDate, chequeNumber, bankName,
//     transferType, transferDate,
//     upiName, upiId, upiDate,
//     paymentDate, paymentTime, paymentDateTime, receiptNo }
//
// refunds:
//   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
//     chequeDate, chequeNumber, bankName,
//     transferType, transferDate,
//     upiName, upiId, upiDate,
//     refundDate, refundTime, refundDateTime, refundReceiptNo }
//

// ---------- GET /api/bills (list) ----------
app.get("/api/bills", async (_req, res) => {
  try {
    const key = makeCacheKey("bills", "list");
    const mapped = await getOrSetCache(key, 30, async () => {
      const snapshot = await db
        .collection("bills")
        .orderBy("invoiceNo", "desc")
        .get();

      return snapshot.docs.map((doc) => {
        const b = doc.data();
        const total = Number(b.total || 0);
        const paidGross = Number(b.paid || 0); // all payments
        const refunded = Number(b.refunded || 0); // all refunds
        const paidNet = paidGross - refunded;
        const balance = b.balance != null ? Number(b.balance) : total - paidNet;

        return {
          id: doc.id,
          invoiceNo: b.invoiceNo || doc.id,
          patientName: b.patientName || "",
          date: b.date || null,
          total,
          paid: paidNet,
          refunded,
          balance,
          status: b.status || "PENDING",
        };
      });
    });

    res.json(mapped);
  } catch (err) {
    console.error("GET /api/bills error:", err);
    res.status(500).json({ error: "Failed to fetch bills" });
  }
});

// ---------- POST /api/bills (create bill + optional first payment) ----------
app.post("/api/bills", async (req, res) => {
  try {
    const {
      patientName,
      sex,
      address,
      age,
      date,
      doctorReg1,
      doctorReg2,
      adjust,
      pay,
      paymentMode,
      referenceNo,

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

      // service rows from CreateBill
      services,
    } = req.body;

    const jsDate = date || new Date().toISOString().slice(0, 10);

    // NORMALIZE services array to a consistent shape
    const normalizedServices = Array.isArray(services)
      ? services.map((s) => {
          const qty = Number(s.qty) || 0;
          const rate = Number(s.rate) || 0;
          const amount = qty * rate;
          return {
            item: s.item || "",
            details: s.details || "",
            qty,
            rate,
            amount,
          };
        })
      : [];

    // ITEMS table data (for legacy items collection)
    const itemsData = normalizedServices.map((s) => {
      const parts = [];
      if (s.item) parts.push(s.item);
      if (s.details) parts.push(s.details);
      const description = parts.join(" - ") || "";
      return {
        description,
        qty: s.qty,
        rate: s.rate,
        amount: s.amount,
      };
    });

    const subtotal = itemsData.reduce((sum, it) => sum + Number(it.amount), 0);
    const adj = Number(adjust) || 0;
    const total = subtotal + adj;

    const firstPay = Number(pay) || 0;
    const refunded = 0;
    const effectivePaid = firstPay - refunded;
    const balance = total - effectivePaid;
    const status = computeStatus(total, effectivePaid);

    // NEW: invoice number based on financial year + sequence
    const { invoiceNo, fy, serial } = await generateInvoiceNumber(jsDate);
    const billId = invoiceNo.replace(/\//g, "_"); // e.g. "25-26/INV-0001"
    const createdAt = new Date().toISOString();

    const billRef = db.collection("bills").doc(billId);

    const batch = db.batch();

    // 1) Bill
    batch.set(billRef, {
      patientName: patientName || "",
      sex: sex || null,
      address: address || "",
      age: age ? Number(age) : null,
      date: jsDate,
      invoiceNo: invoiceNo,
      doctorReg1: doctorReg1 || null,
      doctorReg2: doctorReg2 || null,
      subtotal,
      adjust: adj,
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

    // 2) Items collection (legacy)
    itemsData.forEach((item) => {
      const itemRef = db.collection("items").doc();
      batch.set(itemRef, {
        billId,
        ...item,
      });
    });

    // 3) Optional first payment
    let paymentDoc = null;
    let receiptDoc = null;

    if (firstPay > 0) {
      // NEW: receipt id based on invoice id
      const receiptNo = await generateReceiptId(invoiceNo); // 25-26/INV-0001/Rec-0001
      const paymentId = receiptNo.replace(/\//g, "_");
      const paymentRef = db.collection("payments").doc();
      const now = new Date();
      const paymentDate = jsDate;
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

    // invalidate cache on write
    cache.flushAll();

    // async fire-and-forget (no await)
    syncBillToSheet({
      id: billId,
      invoiceNo: invoiceNo,
      patientName,
      address,
      age: age ? Number(age) : null,
      date: jsDate,
      subtotal,
      adjust: adj,
      total,
      paid: firstPay,
      refunded,
      balance,
      status,
      sex: sex || null,
    });

    syncItemsToSheet(billId, billId, patientName, itemsData);

    res.json({
      bill: {
        id: billId,
        invoiceNo: invoiceNo,
        patientName: patientName || "",
        sex: sex || null,
        address: address || "",
        age: age ? Number(age) : null,
        date: jsDate,
        doctorReg1: doctorReg1 || null,
        doctorReg2: doctorReg2 || null,
        subtotal,
        adjust: adj,
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

// ---------- GET /api/bills/:id (detail + items + payments + refunds) ----------
app.get("/api/bills/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const key = makeCacheKey("bill-detail", id);
    const data = await getOrSetCache(key, 30, async () => {
      const billRef = db.collection("bills").doc(id);
      const billSnap = await billRef.get();
      if (!billSnap.exists) {
        throw new Error("NOT_FOUND");
      }

      const bill = billSnap.data();

      // Items
      const itemsSnap = await db
        .collection("items")
        .where("billId", "==", id)
        .get();

      const items = itemsSnap.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));

      // Payments
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
          date: d.paymentDate || null,
          time: d.paymentTime || null,
          paymentDateTime,
          drawnOn: d.drawnOn || null,
          drawnAs: d.drawnAs || null,
        };
      });

      payments.sort((a, b) => {
        const da = a.paymentDateTime
          ? new Date(a.paymentDateTime)
          : new Date(0);
        const dbb = b.paymentDateTime
          ? new Date(b.paymentDateTime)
          : new Date(0);
        return da - dbb;
      });

      const totalPaidGross = payments.reduce(
        (sum, p) => sum + Number(p.amount || 0),
        0
      );

      // Refunds
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
          date: d.refundDate || null,
          time: d.refundTime || null,
          refundDateTime,
          drawnOn: d.drawnOn || null,
          drawnAs: d.drawnAs || null,
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

      const total = Number(bill.total || 0);
      const netPaid = totalPaidGross - totalRefunded;
      const balance = total - netPaid;
      const status = computeStatus(total, netPaid);

      const primaryPayment = payments[0] || null;

      return {
        id,
        invoiceNo: bill.invoiceNo || id,
        patientName: bill.patientName || "",
        sex: bill.sex || null,
        address: bill.address || "",
        age: bill.age || null,
        date: bill.date || null,
        subtotal: Number(bill.subtotal || 0),
        adjust: Number(bill.adjust || 0),
        total,
        paid: netPaid,
        refunded: totalRefunded,
        totalPaid: totalPaidGross,
        balance,
        status,
        doctorReg1: bill.doctorReg1 || null,
        doctorReg2: bill.doctorReg2 || null,
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

// ---------- POST /api/bills/:id/payments (add partial payment) ----------
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

    // NEW – mode-specific fields
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
    const paymentDate = now.toISOString().slice(0, 10);
    const paymentTime = now.toTimeString().slice(0, 5);
    const paymentDateTime = now.toISOString();

    const invoiceNo = bill.invoiceNo || billId;
    const receiptNo = await generateReceiptId(invoiceNo); // 25-26/INV-0001/REC-0002
    const paymentId = receiptNo.replace(/\//g, "_"); // 25-26_INV-0001_REC-0002
    const paymentRef = db.collection("payments").doc(paymentId);
    const paymentDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,

      // NEW – mode-specific fields persisted
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

    // invalidate cache on write
    cache.flushAll();

    // sync to sheet
    syncPaymentToSheet(
      { id: paymentRef.id, ...paymentDoc },
      { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
    );

    res.status(201).json({
      id: paymentId,
      ...paymentDoc,
    });
  } catch (err) {
    console.error("payment error:", err);
    res.status(500).json({ error: "Payment failed" });
  }
});

// ---------- POST /api/bills/:id/refunds (issue refund) ----------
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

    // NEW – mode-specific fields
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
    const paidGross = Number(bill.paid || 0);
    const alreadyRefunded = Number(bill.refunded || 0);
    const netPaidBefore = paidGross - alreadyRefunded;

    if (numericAmount > netPaidBefore) {
      return res.status(400).json({
        error: "Cannot refund more than net paid amount",
      });
    }

    const now = new Date();
    const refundDate = now.toISOString().slice(0, 10);
    const refundTime = now.toTimeString().slice(0, 5);
    const refundDateTime = now.toISOString();
    const invoiceNo = bill.invoiceNo || billId;
    const refundReceiptNo = await generateRefundId(invoiceNo); // 25-26/INV-0001/REF-0001
    const refundId = refundReceiptNo.replace(/\//g, "_"); // 25-26_INV-0001_REF-0001
    const refundRef = db.collection("refunds").doc(refundId);

    const refundDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,

      // NEW – mode-specific fields persisted
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

    const newRefunded = alreadyRefunded + numericAmount;
    const effectivePaid = paidGross - newRefunded;
    const newBalance = total - effectivePaid;
    const newStatus = computeStatus(total, effectivePaid);

    const batch = db.batch();
    batch.set(refundRef, refundDoc);
    batch.update(billRef, {
      refunded: newRefunded,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();

    // invalidate cache on write
    cache.flushAll();

    syncRefundToSheet(
      {
        id: refundRef.id,
        ...refundDoc,
        netPaidAfterThis: effectivePaid,
        balanceAfterThis: newBalance,
      },
      { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
    );

    res.status(201).json({
      id: refundId,
      ...refundDoc,
    });
  } catch (err) {
    console.error("refund error:", err);
    res.status(500).json({ error: "Refund failed" });
  }
});

// ---------- GET /api/payments/:id (JSON for receipt page) ----------
app.get("/api/payments/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    const key = makeCacheKey("payment-detail", id);
    const data = await getOrSetCache(key, 30, async () => {
      const paymentRef = db.collection("payments").doc(id);
      const paymentSnap = await paymentRef.get();

      if (!paymentSnap.exists) {
        throw new Error("NOT_FOUND");
      }

      const payment = paymentSnap.data();
      const billId = payment.billId;

      const billRef = db.collection("bills").doc(billId);
      const billSnap = await billRef.get();

      if (!billSnap.exists) {
        throw new Error("BILL_NOT_FOUND");
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
          return { id: doc.id, paymentDateTime, amount: Number(d.amount || 0) };
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
        cumulativePaid += Number(p.amount || 0);
        if (p.id === id) {
          paidTillThis = cumulativePaid;
          balanceAfterThis = billTotal - paidTillThis;
          break;
        }
      }

      const itemsSnap = await db
        .collection("items")
        .where("billId", "==", billId)
        .get();

      const items = itemsSnap.docs.map((doc) => {
        const d = doc.data();
        return {
          id: doc.id,
          description: d.description,
          qty: Number(d.qty),
          rate: Number(d.rate),
          amount: Number(d.amount),
        };
      });

      return {
        id,
        amount: Number(payment.amount),
        mode: payment.mode,
        referenceNo: payment.referenceNo,
        drawnOn: payment.drawnOn,
        drawnAs: payment.drawnAs,
        paymentDate: payment.paymentDate,
        receiptNo: payment.receiptNo || `R-${String(id).padStart(4, "0")}`,
        bill: {
          id: billId,
          date: bill.date,
          subtotal: Number(bill.subtotal),
          adjust: Number(bill.adjust),
          total: billTotal,
          paid: paidTillThis,
          balance: balanceAfterThis,
          doctorReg1: bill.doctorReg1,
          doctorReg2: bill.doctorReg2,
          address: bill.address,
          age: bill.age,
          patientName: bill.patientName || "",
          items,
        },
      };
    });

    res.json(data);
  } catch (err) {
    if (err.message === "NOT_FOUND") {
      return res.status(404).json({ error: "Payment not found" });
    }
    if (err.message === "BILL_NOT_FOUND") {
      return res.status(404).json({ error: "Bill not found" });
    }
    console.error("GET /api/payments/:id error:", err);
    res.status(500).json({ error: "Failed to load payment" });
  }
});

// ---------- PDF: Invoice (A4 full page) ----------
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

    // 3) FINAL ITEMS
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
      .sort((a, b) => {
        const da = a.paymentDateTime
          ? new Date(a.paymentDateTime)
          : new Date(0);
        const dbb = b.paymentDateTime
          ? new Date(b.paymentDateTime)
          : new Date(0);
        return da - dbb;
      });

    const primaryPayment = payments[0] || null;

    const totalPaidGross = payments.reduce(
      (sum, p) => sum + Number(p.amount || 0),
      0
    );

    // 5) REFUNDS
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
    let subtotal = Number(bill.subtotal || 0);
    let adjust = Number(bill.adjust || 0);
    let total = Number(bill.total || 0);

    if (!subtotal && items.length > 0) {
      subtotal = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
    }
    if (!total && subtotal) {
      total = subtotal + adjust;
    }

    const paidNet = totalPaidGross - totalRefunded;
    const balance = total - paidNet;

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const invoiceNo = bill.invoiceNo || id;
    const dateText = bill.date || "";
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

    doc
      .font("Helvetica-Bold")
      .fontSize(16)
      .text("MADHUREKHA EYE CARE CENTRE", 0, y + 4, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(
        "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 24,
        { align: "center", width: pageWidth }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 60;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();

    y += 4;

    // static doctor names + reg nos
    doc.fontSize(9).font("Helvetica-Bold");
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text("Reg. No.: 28873", 36, y);
    doc.text("Reg. No.: 16219", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

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

    // 8) SERVICES TABLE
    const tableLeft = 36;
    const colSrW = 22;
    const colQtyW = 48;
    const colRateW = 70;
    const colAdjW = 60;
    const colSubW = 70;
    const colServiceW =
      usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

    const colSrX = tableLeft;
    const colQtyX = colSrX + colSrW;
    const colServiceX = colQtyX + colQtyW;
    const colRateX = colServiceX + colServiceW;
    const colAdjX = colRateX + colRateW;
    const colSubX = colAdjX + colAdjW;

    // header background
    doc
      .save()
      .rect(tableLeft, y, usableWidth, 16)
      .fill("#F3F3F3")
      .restore()
      .rect(tableLeft, y, usableWidth, 16)
      .stroke();

    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Sr.", colSrX + 2, y + 3);
    doc.text("Qty", colQtyX + 2, y + 3);
    doc.text("Procedure", colServiceX + 2, y + 3, {
      width: colServiceW - 4,
    });
    doc.text("Rate / Price", colRateX + 2, y + 3, {
      width: colRateW - 4,
      align: "right",
    });
    doc.text("Adjust", colAdjX + 2, y + 3, {
      width: colAdjW - 4,
      align: "right",
    });
    doc.text("Sub Total", colSubX + 2, y + 3, {
      width: colSubW - 4,
      align: "right",
    });

    y += 16;
    doc.font("Helvetica").fontSize(9);

    items.forEach((item, idx) => {
      const rowHeight = 14;

      doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

      doc.text(String(idx + 1), colSrX + 2, y + 3);
      doc.text(
        item.qty != null && item.qty !== "" ? String(item.qty) : "",
        colQtyX + 2,
        y + 3
      );
      doc.text(item.description || "", colServiceX + 2, y + 3, {
        width: colServiceW - 4,
      });
      doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
        width: colRateW - 4,
        align: "right",
      });
      doc.text("0.00", colAdjX + 2, y + 3, {
        width: colAdjW - 4,
        align: "right",
      });
      doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
        width: colSubW - 4,
        align: "right",
      });

      y += rowHeight;

      if (y > doc.page.height - 200) {
        doc.addPage();
        y = 36;
      }
    });

    y += 12;

    // 9) TOTALS BOX
    const boxWidth = 180;
    const boxX = pageWidth - 36 - boxWidth;
    const boxY = y;
    const lineH = 12;

    doc.rect(boxX, boxY, boxWidth, lineH * 5 + 4).stroke();

    doc.fontSize(9).font("Helvetica");

    doc.text("Sub Total", boxX + 6, boxY + 2);
    doc.text(`Rs ${formatMoney(subtotal)}`, boxX, boxY + 2, {
      width: boxWidth - 6,
      align: "right",
    });

    doc.text("Adjust", boxX + 6, boxY + 2 + lineH);
    doc.text(`Rs ${formatMoney(adjust)}`, boxX, boxY + 2 + lineH, {
      width: boxWidth - 6,
      align: "right",
    });

    doc.text("Tax", boxX + 6, boxY + 2 + lineH * 2);
    doc.text("Rs 0.00", boxX, boxY + 2 + lineH * 2, {
      width: boxWidth - 6,
      align: "right",
    });

    doc.text("Refunded", boxX + 6, boxY + 2 + lineH * 3);
    doc.text(`Rs ${formatMoney(totalRefunded)}`, boxX, boxY + 2 + lineH * 3, {
      width: boxWidth - 6,
      align: "right",
    });

    doc.font("Helvetica-Bold");
    doc.text("Total Due", boxX + 6, boxY + 2 + lineH * 4);
    doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2 + lineH * 4, {
      width: boxWidth - 6,
      align: "right",
    });

    // move below totals box
    y = boxY + lineH * 5 + 20;

    // NET PAID + BALANCE
    doc.font("Helvetica").fontSize(9);
    doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
    doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 18;

    // PAYMENT DETAILS BLOCK
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
    doc.fontSize(8).text("Patient / Representative", 36, sigY + 4, {
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
    doc
      .fontSize(8)
      .text("For Madhurekha Eye Care Centre", rightSigX2, sigY + 4, {
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
      .text("MADHUREKHA EYE CARE CENTRE", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(
        "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 20,
        {
          align: "center",
          width: pageWidth,
        }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 48;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;

    // DOCTOR LINE (static)
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text("Reg. No.: 28873", 36, y);
    doc.text("Reg. No.: 16219", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

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
    const isPayment = true;
    const leftX = 36;
    const rightBoxWidth = 180;
    const rightX = pageWidth - 36 - rightBoxWidth;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Receipt No.: ${receiptNo}`, leftX, y);
    doc.text(`Date: ${payment.paymentDate || ""}`, rightX, y, {
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

    let detailsY = doc.y + 4;
    doc.font("Helvetica");

    const addDetail = (label, value) => {
      if (!value) return;
      doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
      detailsY = doc.y + 3;
    };

    addDetail("Amount Received: Rs", formatMoney(payment.amount));
    addDetail("Payment Mode:", mode);
    addDetail("Reference No.:", referenceNo);
    addDetail("Drawn On:", drawnOn);
    addDetail("Drawn As:", drawnAs);
    addDetail("Cheque No.:", chequeNumber);
    addDetail("Cheque Date:", chequeDate);
    addDetail("Bank:", bankName);
    addDetail("Transfer Type:", transferType);
    addDetail("Transfer Date:", transferDate);
    addDetail("UPI ID:", upiId);
    addDetail("UPI Name:", upiName);
    addDetail("UPI Date:", upiDate);

    // RIGHT BILL SUMMARY BOX
    const boxY = detailsTopY;
    const lineH = 12;
    const rows = 5; // Bill No, Date, Total, Paid, Balance
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
    addRow("Bill Date:", bill.date || "");
    addRow("Bill Total:", `Rs ${formatMoney(billTotal)}`);
    addRow("Paid (incl. this):", `Rs ${formatMoney(paidTillThis)}`);
    addRow("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

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

    doc
      .moveTo(leftX, sigY)
      .lineTo(leftX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text("Patient / Representative", leftX, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    const rightSigX = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc
      .fontSize(8)
      .text("For Madhurekha Eye Care Centre", rightSigX, sigY + 4, {
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

// ---------- PDF: Refund Receipt (A4 half page, professional layout) ----------
app.get("/api/refunds/:id/refund-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid refund id" });

  try {
    const refundRef = db.collection("refunds").doc(id);
    const refundSnap = await refundRef.get();
    if (!refundSnap.exists) {
      return res.status(404).json({ error: "Refund not found" });
    }
    const refund = refundSnap.data();
    const billId = refund.billId;

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

    const totalPaidGross = allPayments.reduce(
      (sum, p) => sum + Number(p.amount || 0),
      0
    );

    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", billId)
      .get();

    const allRefunds = refundsSnap.docs
      .map((doc) => {
        const d = doc.data();
        const refundDateTime =
          d.refundDateTime ||
          (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
        return {
          id: doc.id,
          refundDateTime,
          amount: Number(d.amount || 0),
        };
      })
      .sort((a, b) => {
        const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
        const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
        return da - dbb;
      });

    let cumulativeRefund = 0;
    let refundedTillThis = 0;
    let balanceAfterThis = billTotal;

    for (const r of allRefunds) {
      cumulativeRefund += r.amount;
      if (r.id === id) {
        refundedTillThis = cumulativeRefund;
        const netPaidAfterThis = totalPaidGross - refundedTillThis;
        balanceAfterThis = billTotal - netPaidAfterThis;
        break;
      }
    }

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const patientName = bill.patientName || "";
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

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="refund-${id}.pdf"`);

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
      .text("MADHUREKHA EYE CARE CENTRE", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(
        "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 20,
        {
          align: "center",
          width: pageWidth,
        }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 48;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;

    // DOCTOR LINE
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text("Reg. No.: 28873", 36, y);
    doc.text("Reg. No.: 16219", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

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
      .text("REFUND RECEIPT", 36, y + 4, {
        align: "center",
        width: usableWidth,
      });

    y += 26;

    // COMMON LAYOUT (left details + right summary)
    const leftX = 36;
    const rightBoxWidth = 180;
    const rightX = pageWidth - 36 - rightBoxWidth;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Refund No.: ${refundNo}`, leftX, y);
    doc.text(`Date: ${refund.refundDate || ""}`, rightX, y, {
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

    let detailsY = doc.y + 4;
    doc.font("Helvetica");

    const addDetail = (label, value) => {
      if (!value) return;
      doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
      detailsY = doc.y + 3;
    };

    addDetail("Amount Refunded: Rs", formatMoney(refund.amount));
    addDetail("Refund Mode:", mode);
    addDetail("Reference No.:", referenceNo);
    addDetail("Drawn On:", drawnOn);
    addDetail("Drawn As:", drawnAs);
    addDetail("Cheque No.:", chequeNumber);
    addDetail("Cheque Date:", chequeDate);
    addDetail("Bank:", bankName);
    addDetail("Transfer Type:", transferType);
    addDetail("Transfer Date:", transferDate);
    addDetail("UPI ID:", upiId);
    addDetail("UPI Name:", upiName);
    addDetail("UPI Date:", upiDate);

    // RIGHT BILL SUMMARY
    const boxY = detailsTopY;
    const lineH = 12;
    const rows = 6; // Bill No, Date, Total, Total Paid, Refunded, Balance
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
    addRow("Bill Date:", bill.date || "");
    addRow("Bill Total:", `Rs ${formatMoney(billTotal)}`);
    addRow("Total Paid:", `Rs ${formatMoney(totalPaidGross)}`);
    addRow("Refunded (incl. this):", `Rs ${formatMoney(refundedTillThis)}`);
    addRow("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

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

    doc
      .moveTo(leftX, sigY)
      .lineTo(leftX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text("Patient / Representative", leftX, sigY + 4, {
      width: sigWidth,
      align: "center",
    });

    const rightSigX = pageWidth - 36 - sigWidth;
    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc
      .fontSize(8)
      .text("For Madhurekha Eye Care Centre", rightSigX, sigY + 4, {
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

// ---------- PDF: Bill Summary (A4 half page) ----------
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

    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();

    const payments = paysSnap.docs.map((doc) => {
      const d = doc.data();
      const paymentDateTime =
        d.paymentDateTime ||
        (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
      return {
        id: doc.id,
        amount: Number(d.amount || 0),
        paymentDateTime,
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

    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", billId)
      .get();

    const refunds = refundsSnap.docs.map((doc) => {
      const d = doc.data();
      const refundDateTime =
        d.refundDateTime ||
        (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
      return {
        id: doc.id,
        amount: Number(d.amount || 0),
        refundDateTime,
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
    const invoiceNo = bill.invoiceNo || billId;
    const billDate = bill.date || "";
    const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

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
      .text("MADHUREKHA EYE CARE CENTRE", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(
        "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 20,
        {
          align: "center",
          width: pageWidth,
        }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 48;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;

    // static doctor header
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text("Reg. No.: 28873", 36, y);
    doc.text("Reg. No.: 16219", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 16;

    // title bar
    doc.rect(36, y, usableWidth, 16).stroke();
    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("BILL SUMMARY", 36, y + 3, {
        align: "center",
        width: usableWidth,
      });

    y += 24;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(`Date: ${billDate}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

    doc.text(`Patient Name: ${patientName}`, 36, y, {
      width: usableWidth,
    });

    y += 18;

    const boxWidth = 260;
    const boxX = 36;
    const boxY = y;
    const lineH = 12;
    const rows = 8;
    const boxHeight = lineH * rows + 8;

    doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

    let by = boxY + 4;

    doc.font("Helvetica").fontSize(9);

    function row(label, value) {
      doc.text(label, boxX + 6, by);
      doc.text(value, boxX + 6, by, {
        width: boxWidth - 12,
        align: "right",
      });
      by += lineH;
    }

    row("Bill Total", `Rs ${formatMoney(billTotal)}`);
    row("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
    row("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
    row("Net Paid", `Rs ${formatMoney(netPaid)}`);
    row("Balance", `Rs ${formatMoney(balance)}`);
    row("Payments Count", String(paymentsCount));
    row("Refunds Count", String(refundsCount));
    row("Status", status);

    const rightSigWidth = 160;
    const sigY = boxY + boxHeight + 30;
    const rightSigX = pageWidth - 36 - rightSigWidth;

    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + rightSigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc
      .fontSize(8)
      .text("For Madhurekha Eye Care Centre", rightSigX, sigY + 4, {
        width: rightSigWidth,
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

app.get("/api/dashboard/summary", async (_req, res) => {
  try {
    const key = makeCacheKey("dashboard", "summary");
    const data = await getOrSetCache(key, 60, async () => {
      const now = new Date();
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, "0");
      const dd = String(now.getDate()).padStart(2, "0");

      const todayStr = `${yyyy}-${mm}-${dd}`;
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
        .where("paymentDate", "==", todayStr)
        .get();
      let todayPayTotal = 0;
      let todayPayCount = 0;
      todayPaymentsSnap.forEach((doc) => {
        todayPayTotal += Number(doc.data().amount || 0);
        todayPayCount++;
      });

      const todayRefundsSnap = await db
        .collection("refunds")
        .where("refundDate", "==", todayStr)
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
          label: todayStr,
          paymentsTotal: todayPayTotal,
          paymentsCount: todayPayCount,
          refundsTotal: todayRefundTotal,
          refundsCount: todayRefundCount,
          netTotal: todayNet,
        },
        month: {
          label: `${yyyy}-${mm}`,
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

// ---------- PDF: Bill Summary (A4 half page with chronological table) ----------
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
      const paymentDateTime =
        d.paymentDateTime ||
        (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
      return {
        id: doc.id,
        amount: Number(d.amount || 0),
        paymentDateTime,
        mode: d.mode || "",
        referenceNo: d.referenceNo || null,
        receiptNo: d.receiptNo || null,
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
      const refundDateTime =
        d.refundDateTime ||
        (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
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
    const invoiceNo = bill.invoiceNo || billId;
    const billDate = bill.date || "";
    const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    function formatDateTime(dtString) {
      if (!dtString) return "";
      const d = new Date(dtString);
      if (Number.isNaN(d.getTime())) return "";
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const mi = String(d.getMinutes()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
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
      timeline.push({
        type: "PAYMENT",
        label: p.receiptNo ? `Payment Receipt (${p.receiptNo})` : "Payment",
        dateTime: p.paymentDateTime,
        mode: p.mode || "",
        ref: p.referenceNo || "",
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
      .text("MADHUREKHA EYE CARE CENTRE", 0, y + 2, {
        align: "center",
        width: pageWidth,
      });

    doc
      .font("Helvetica")
      .fontSize(9)
      .text(
        "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 20,
        {
          align: "center",
          width: pageWidth,
        }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 48;

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();
    y += 6;

    // static doctor header
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text("Reg. No.: 28873", 36, y);
    doc.text("Reg. No.: 16219", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

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
    doc.text(`Date: ${billDate}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

    doc.text(`Patient Name: ${patientName}`, 36, y, {
      width: usableWidth,
    });

    y += 18;

    // --------- CHRONOLOGICAL TABLE ---------
    const tableLeft = 36;
    const colDateW = 80;
    const colPartW = 150;
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

    // header background
    doc
      .save()
      .rect(tableLeft, y, usableWidth, 16)
      .fill("#F3F3F3")
      .restore()
      .rect(tableLeft, y, usableWidth, 16)
      .stroke();

    doc.font("Helvetica-Bold").fontSize(8);
    doc.text("Date & Time", colDateX + 2, y + 3, {
      width: colDateW - 4,
    });
    doc.text("Particulars", colPartX + 2, y + 3, {
      width: colPartW - 4,
    });
    doc.text("Mode", colModeX + 2, y + 3, {
      width: colModeW - 4,
    });
    doc.text("Reference", colRefX + 2, y + 3, {
      width: colRefW - 4,
    });
    doc.text("Debit (Rs)", colDebitX + 2, y + 3, {
      width: colDebitW - 4,
      align: "right",
    });
    doc.text("Credit (Rs)", colCreditX + 2, y + 3, {
      width: colCreditW - 4,
      align: "right",
    });
    doc.text("Balance (Rs)", colBalX + 2, y + 3, {
      width: colBalW - 4,
      align: "right",
    });

    y += 16;
    doc.font("Helvetica").fontSize(8);

    let runningBalance = 0;

    timeline.forEach((ev) => {
      const rowHeight = 14;

      doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

      if (ev.type === "INVOICE") {
        runningBalance = ev.debit - ev.credit;
      } else {
        runningBalance += ev.debit;
        runningBalance -= ev.credit;
      }

      doc.text(formatDateTime(ev.dateTime), colDateX + 2, y + 3, {
        width: colDateW - 4,
      });
      doc.text(ev.label || "", colPartX + 2, y + 3, {
        width: colPartW - 4,
      });
      doc.text(ev.mode || "", colModeX + 2, y + 3, {
        width: colModeW - 4,
      });
      doc.text(ev.ref || "", colRefX + 2, y + 3, {
        width: colRefW - 4,
      });
      doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 2, y + 3, {
        width: colDebitW - 4,
        align: "right",
      });
      doc.text(ev.credit ? formatMoney(ev.credit) : "", colCreditX + 2, y + 3, {
        width: colCreditW - 4,
        align: "right",
      });
      doc.text(formatMoney(runningBalance), colBalX + 2, y + 3, {
        width: colBalW - 4,
        align: "right",
      });

      y += rowHeight;
    });

    y += 18;

    // --------- TOTALS BOX ---------
    const boxWidth = 260;
    const boxX = 36;
    const boxY = y;
    const lineH2 = 12;
    const rows2 = 8;
    const boxHeight = lineH2 * rows2 + 8;

    doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

    let by = boxY + 4;

    doc.font("Helvetica").fontSize(9);

    function row(label, value) {
      doc.text(label, boxX + 6, by);
      doc.text(value, boxX + 6, by, {
        width: boxWidth - 12,
        align: "right",
      });
      by += lineH2;
    }

    row("Bill Total", `Rs ${formatMoney(billTotal)}`);
    row("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
    row("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
    row("Net Paid", `Rs ${formatMoney(netPaid)}`);
    row("Balance", `Rs ${formatMoney(balance)}`);
    row("Payments Count", String(paymentsCount));
    row("Refunds Count", String(refundsCount));
    row("Status", status);

    const rightSigWidth = 160;
    const sigY = boxY + boxHeight + 30;
    const rightSigX = pageWidth - 36 - rightSigWidth;

    doc
      .moveTo(rightSigX, sigY)
      .lineTo(rightSigX + rightSigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc
      .fontSize(8)
      .text("For Madhurekha Eye Care Centre", rightSigX, sigY + 4, {
        width: rightSigWidth,
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

// ---------- PUT /api/bills/:id (edit bill: patient + services, NOT payments) ----------
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
      doctorReg1,
      doctorReg2,
      adjust,
      remarks,
      services,
    } = req.body;

    const jsDate =
      date || oldBill.date || new Date().toISOString().slice(0, 10);

    // --- NORMALIZE SERVICES (same style as POST /api/bills) ---
    const normalizedServices = Array.isArray(services)
      ? services.map((s) => {
          const qty = Number(s.qty) || 0;
          const rate = Number(s.rate) || 0;
          const amount = qty * rate;
          return {
            item: s.item || "",
            details: s.details || "",
            qty,
            rate,
            amount,
          };
        })
      : [];

    const itemsData = normalizedServices.map((s) => {
      const parts = [];
      if (s.item) parts.push(s.item);
      if (s.details) parts.push(s.details);
      const description = parts.join(" - ") || "";
      return {
        description,
        qty: s.qty,
        rate: s.rate,
        amount: s.amount,
      };
    });

    const subtotal = itemsData.reduce(
      (sum, it) => sum + Number(it.amount || 0),
      0
    );
    const adj = Number(adjust ?? oldBill.adjust ?? 0) || 0;
    const total = subtotal + adj;

    // keep payments/refunds as is
    const paidGross = Number(oldBill.paid || 0);
    const refunded = Number(oldBill.refunded || 0);
    const effectivePaid = paidGross - refunded;
    const balance = total - effectivePaid;
    const status = computeStatus(total, effectivePaid);

    const batch = db.batch();

    // 1) Update bill doc
    batch.update(billRef, {
      patientName: patientName ?? oldBill.patientName ?? "",
      sex: sex ?? oldBill.sex ?? null,
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      doctorReg1: doctorReg1 ?? oldBill.doctorReg1 ?? null,
      doctorReg2: doctorReg2 ?? oldBill.doctorReg2 ?? null,
      subtotal,
      adjust: adj,
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
    const existingItemsSnap = await db
      .collection("items")
      .where("billId", "==", billId)
      .get();

    existingItemsSnap.forEach((doc) => {
      batch.delete(doc.ref);
    });

    itemsData.forEach((item) => {
      const itemRef = db.collection("items").doc();
      batch.set(itemRef, {
        billId,
        ...item,
      });
    });

    await batch.commit();

    // clear caches so GET /api/bills and /api/bills/:id show updated data
    cache.flushAll();

    // update Google Sheet (optional but consistent with create)
    syncBillToSheet({
      id: billId,
      invoiceNo: oldBill.invoiceNo || billId,
      patientName: patientName ?? oldBill.patientName ?? "",
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      subtotal,
      adjust: adj,
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
      patientName ?? oldBill.patientName ?? "",
      itemsData
    );

    res.json({
      id: billId,
      invoiceNo: oldBill.invoiceNo || billId,
      patientName: patientName ?? oldBill.patientName ?? "",
      sex: sex ?? oldBill.sex ?? null,
      address: address ?? oldBill.address ?? "",
      age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
      date: jsDate,
      subtotal,
      adjust: adj,
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

// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
