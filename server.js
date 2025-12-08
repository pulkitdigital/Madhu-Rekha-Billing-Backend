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

async function generateReceiptNumber() {
  const now = Date.now();
  const random = Math.floor(Math.random() * 1000);
  const suffix = `${String(now).slice(-6)}${String(random).padStart(3, "0")}`;
  return `RCP-${suffix}`;
}

async function generateRefundNumber() {
  const now = Date.now();
  const random = Math.floor(Math.random() * 1000);
  const suffix = `${String(now).slice(-6)}${String(random).padStart(3, "0")}`;
  return `RFD-${suffix}`;
}

function formatDateYYYYMMDD(dateStrOrDate) {
  const d = dateStrOrDate ? new Date(dateStrOrDate) : new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

// Turn "Rohit Sharma" => "ROHITSHAR"
function makeNameSlug(name) {
  if (!name) return "NONAME";
  const slug = name
    .toUpperCase()
    .replace(/[^A-Z]/g, "")
    .slice(0, 8);
  return slug || "NONAME";
}

// Bill id + invoiceNo: INV-YYYYMMDD-HHMMSS-NAME
function generateBillId(patientName, billDateInput) {
  const datePart = formatDateYYYYMMDD(billDateInput);
  const now = new Date();
  const timePart = `${String(now.getHours()).padStart(2, "0")}${String(
    now.getMinutes()
  ).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
  const nameSlug = makeNameSlug(patientName);
  return `INV-${datePart}-${timePart}-${nameSlug}`;
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
//   { patientName, address, age, date, invoiceNo, subtotal, adjust,
//     total, paid, refunded, balance, doctorReg1, doctorReg2,
//     status, createdAt }
//
// items:
//   { billId, description, qty, rate, amount }
//
// payments:
//   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
//     paymentDate, paymentTime, paymentDateTime, receiptNo }
//
// refunds:
//   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
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
        const balance =
          b.balance != null ? Number(b.balance) : total - paidNet;

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
      address,
      age,
      date,
      doctorReg1,
      doctorReg2,
      adjust,
      pay,
      paymentMode,
      referenceNo,
      drawnOn,
      drawnAs,
      services,
    } = req.body;

    const jsDate = date || new Date().toISOString().slice(0, 10);

    const itemsData = (services || []).map((s) => {
      const qty = Number(s.qty) || 0;
      const rate = Number(s.rate) || 0;
      const amount = qty * rate;
      return {
        description: s.description || "",
        qty,
        rate,
        amount,
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

    const billId = generateBillId(patientName, jsDate);
    const createdAt = new Date().toISOString();

    const billRef = db.collection("bills").doc(billId);
    const batch = db.batch();

    // 1) Bill
    batch.set(billRef, {
      patientName: patientName || "",
      address: address || "",
      age: age ? Number(age) : null,
      date: jsDate,
      invoiceNo: billId,
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
    });

    // 2) Items
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
      const receiptNo = await generateReceiptNumber();
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
        paymentDate,
        paymentTime,
        paymentDateTime,
        receiptNo,
      };
      batch.set(paymentRef, paymentDoc);
      receiptDoc = { receiptNo };
    }

    await batch.commit();

    // invalidate cache on write
    cache.flushAll();
    // async fire-and-forget (no await if you don't care about result blocking request)
syncBillToSheet({
  id: billId,
  invoiceNo: billId,
  patientName,
  address,
  age,
  date: jsDate,
  doctorReg1,
  doctorReg2,
  subtotal,
  adjust: adj,
  total,
  paid: firstPay,
  refunded,
  balance,
  status,
});

syncItemsToSheet(billId, billId, patientName, itemsData);

    res.json({
      bill: {
        id: billId,
        invoiceNo: billId,
        patientName: patientName || "",
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
        items: itemsData,
        paymentMode: paymentDoc?.mode || null,
        referenceNo: paymentDoc?.referenceNo || null,
        drawnOn: paymentDoc?.drawnOn || null,
        drawnAs: paymentDoc?.drawnAs || null,
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
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
        const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
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
        const dbb = b.refundDateTime
          ? new Date(b.refundDateTime)
          : new Date(0);
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

  const { amount, mode, referenceNo, drawnOn, drawnAs } = req.body;
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
    const receiptNo = await generateReceiptNumber();

    const paymentRef = db.collection("payments").doc();
    const paymentDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,
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

    // reload minimal bill info for sheet row
syncPaymentToSheet(
  { id: paymentRef.id, ...paymentDoc },
  { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
);


    res.status(201).json({
      id: paymentRef.id,
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

  const { amount, mode, referenceNo, drawnOn, drawnAs } = req.body;
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
    const refundReceiptNo = await generateRefundNumber();

    const refundRef = db.collection("refunds").doc();
    const refundDoc = {
      billId,
      amount: numericAmount,
      mode: mode || "Cash",
      referenceNo: referenceNo || null,
      drawnOn: drawnOn || null,
      drawnAs: drawnAs || null,
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
  { id: refundRef.id, ...refundDoc },
  { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
);
    res.status(201).json({
      id: refundRef.id,
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
          const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
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

// ---------- GET /api/bills/:id/invoice-html-pdf (Invoice – A4 full page) ----------
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

    const itemsSnap = await db
      .collection("items")
      .where("billId", "==", id)
      .get();

    const items = itemsSnap.docs.map((doc) => ({
      id: doc.id,
      ...doc.data(),
    }));

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
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
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

    const refundsSnap = await db
      .collection("refunds")
      .where("billId", "==", id)
      .get();

    const refunds = refundsSnap.docs.map((doc) => {
      const d = doc.data();
      return Number(d.amount || 0);
    });

    const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

    const subtotal = Number(bill.subtotal || 0);
    const adjust = Number(bill.adjust || 0);
    const total = Number(bill.total || 0);
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

    const paymentMode = primaryPayment?.mode || "Cash";
    const referenceNo = primaryPayment?.referenceNo || null;
    const drawnOn = primaryPayment?.drawnOn || null;
    const drawnAs = primaryPayment?.drawnAs || null;

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
      doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
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
        "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
        0,
        y + 24,
        { align: "center", width: pageWidth }
      )
      .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
        align: "center",
        width: pageWidth,
      });

    y += 60;

    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();

    y += 4;

    doc.fontSize(9).font("Helvetica-Bold");
    doc.text("Dr. Pradipta Kundu", 36, y);
    doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;
    doc.font("Helvetica").fontSize(8);
    doc.text(`Reg. No.: ${bill.doctorReg1 || "________"}`, 36, y);
    doc.text(
      `Reg. No.: ${bill.doctorReg2 || "________"}`,
      pageWidth / 2,
      y,
      {
        align: "right",
        width: usableWidth / 2,
      }
    );

    y += 16;

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

    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

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

    doc.text(
      `Address: ${bill.address || "________________________"}`,
      36,
      y,
      { width: usableWidth }
    );

    y += 20;

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

    doc.rect(tableLeft, y, usableWidth, 16).stroke();

    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Sr.", colSrX + 2, y + 3);
    doc.text("Hrs / Qty", colQtyX + 2, y + 3);
    doc.text("Service", colServiceX + 2, y + 3, {
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
      const qty = Number(item.qty || 0);
      const rate = Number(item.rate || 0);
      const amount = Number(item.amount || qty * rate);
      const rowHeight = 14;

      doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

      doc.text(String(idx + 1), colSrX + 2, y + 3);
      doc.text(String(qty || ""), colQtyX + 2, y + 3);
      doc.text(item.description || "", colServiceX + 2, y + 3, {
        width: colServiceW - 4,
      });
      doc.text(formatMoney(rate), colRateX + 2, y + 3, {
        width: colRateW - 4,
        align: "right",
      });
      doc.text("0.00", colAdjX + 2, y + 3, {
        width: colAdjW - 4,
        align: "right",
      });
      doc.text(formatMoney(amount), colSubX + 2, y + 3, {
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

    doc.font("Helvetica");
    y = boxY + lineH * 5 + 20;

    doc.fontSize(9);
    doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
    doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 22;

    const fullWidth = usableWidth;

    let line = `Received with thanks from Shri/Smt./M/s ${patientName} the sum of Rupees Rs ${formatMoney(
      paidNet
    )} dated ${dateText} by ${paymentMode || "________"} / Bank / Transfer / Cheque No. / UPI`;
    if (referenceNo) {
      line += ` (${referenceNo})`;
    }
    line += ".";

    doc.fontSize(9).text(line, 36, y, { width: fullWidth });

    y = doc.y + 6;

    const drawnOnText = drawnOn || "________________________";
    const drawnAsText = drawnAs || "________________________";

    doc.text(
      `Drawn on ${drawnOnText} (Subject to realization) as ${drawnAsText}.`,
      36,
      y,
      { width: fullWidth }
    );

    y = doc.y + 6;

    doc
      .fontSize(8)
      .text(
        "* Dispute if any Subject to Jamshedpur Jurisdiction",
        36,
        y,
        { width: fullWidth }
      );

    y = doc.y + 30;

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
    console.error("invoice-html-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate invoice PDF" });
    }
  }
});

// ---------- PDF: Payment Receipt (A4 half page) ----------
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

    const paysSnap = await db
      .collection("payments")
      .where("billId", "==", billId)
      .get();

    const billTotal = Number(bill.total || 0);

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
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
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
    const receiptNo =
      payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

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
        "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
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

    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();

    y += 6;

    doc.rect(36, y, usableWidth, 16).stroke();

    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("PAYMENT RECEIPT", 36, y + 3, {
        align: "center",
        width: usableWidth,
      });

    y += 24;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Receipt No.: ${receiptNo}`, 36, y);
    doc.text(`Date: ${payment.paymentDate || ""}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 16;

    const leftBlockX = 36;
    const leftBlockWidth = usableWidth - 190;
    const rightBoxWidth = 170;
    const rightBoxX = pageWidth - 36 - rightBoxWidth;

    const textYStart = y;

    doc
      .font("Helvetica-Bold")
      .text(`Patient Name: ${patientName}`, leftBlockX, y, {
        width: leftBlockWidth,
      });

    y = doc.y + 6;
    doc.font("Helvetica");

    doc.text(
      `Amount Received: Rs ${formatMoney(payment.amount)}`,
      leftBlockX,
      y,
      {
        width: leftBlockWidth,
      }
    );
    y = doc.y + 4;

    doc.text(`Payment Mode: ${mode}`, leftBlockX, y, {
      width: leftBlockWidth,
    });
    y = doc.y + 4;

    if (referenceNo) {
      doc.text(`Reference No: ${referenceNo}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    if (drawnOn) {
      doc.text(`Drawn On: ${drawnOn}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    if (drawnAs) {
      doc.text(`Instrument: ${drawnAs}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    y = doc.y + 8;

    doc.fontSize(8).text(
      "* Dispute if any Subject to Jamshedpur Jurisdiction",
      leftBlockX,
      y,
      { width: leftBlockWidth }
    );

    const boxY = textYStart;
    const lineH = 12;
    const boxHeight = lineH * 6 + 6;

    doc.rect(rightBoxX, boxY, rightBoxWidth, boxHeight).stroke();

    let by = boxY + 4;

    doc.font("Helvetica-Bold").fontSize(9).text("Bill Summary", rightBoxX + 6, by);
    by += lineH + 2;

    doc.font("Helvetica").fontSize(9);

    const billNoText = bill.invoiceNo || billId;
    doc.text(`Bill No.: ${billNoText}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(`Bill Date: ${bill.date || ""}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(`Bill Total: Rs ${formatMoney(billTotal)}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(
      `Paid (incl. this): Rs ${formatMoney(paidTillThis)}`,
      rightBoxX + 6,
      by
    );
    by += lineH;

    doc.text(
      `Balance: Rs ${formatMoney(balanceAfterThis)}`,
      rightBoxX + 6,
      by
    );

    const sigY = boxY + boxHeight + 40;
    const sigWidth = 160;

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

// ---------- PDF: Refund Receipt (A4 half page) ----------
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
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
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
        const dbb = b.refundDateTime
          ? new Date(b.refundDateTime)
          : new Date(0);
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

    const netPaidAfterThis = totalPaidGross - refundedTillThis;

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
        "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
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

    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();

    y += 6;

    doc.rect(36, y, usableWidth, 16).stroke();

    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("REFUND RECEIPT", 36, y + 3, {
        align: "center",
        width: usableWidth,
      });

    y += 24;

    doc.font("Helvetica").fontSize(9);
    doc.text(`Refund No.: ${refundNo}`, 36, y);
    doc.text(`Date: ${refund.refundDate || ""}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 16;

    const leftBlockX = 36;
    const leftBlockWidth = usableWidth - 190;
    const rightBoxWidth = 170;
    const rightBoxX = pageWidth - 36 - rightBoxWidth;

    const textYStart = y;

    doc
      .font("Helvetica-Bold")
      .text(`Patient Name: ${patientName}`, leftBlockX, y, {
        width: leftBlockWidth,
      });

    y = doc.y + 6;
    doc.font("Helvetica");

    doc.text(
      `Amount Refunded: Rs ${formatMoney(refund.amount)}`,
      leftBlockX,
      y,
      {
        width: leftBlockWidth,
      }
    );
    y = doc.y + 4;

    doc.text(`Refund Mode: ${mode}`, leftBlockX, y, {
      width: leftBlockWidth,
    });
    y = doc.y + 4;

    if (referenceNo) {
      doc.text(`Reference No: ${referenceNo}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    if (drawnOn) {
      doc.text(`Drawn On: ${drawnOn}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    if (drawnAs) {
      doc.text(`Instrument: ${drawnAs}`, leftBlockX, y, {
        width: leftBlockWidth,
      });
      y = doc.y + 4;
    }

    y = doc.y + 8;

    doc.fontSize(8).text(
      "* Dispute if any Subject to Jamshedpur Jurisdiction",
      leftBlockX,
      y,
      { width: leftBlockWidth }
    );

    const boxY = textYStart;
    const lineH = 12;
    const boxHeight = lineH * 6 + 6;

    doc.rect(rightBoxX, boxY, rightBoxWidth, boxHeight).stroke();

    let by = boxY + 4;

    doc.font("Helvetica-Bold").fontSize(9).text("Bill Summary", rightBoxX + 6, by);
    by += lineH + 2;

    doc.font("Helvetica").fontSize(9);

    const billNoText = bill.invoiceNo || billId;
    doc.text(`Bill No.: ${billNoText}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(`Bill Date: ${bill.date || ""}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(`Bill Total: Rs ${formatMoney(billTotal)}`, rightBoxX + 6, by);
    by += lineH;

    doc.text(
      `Total Paid: Rs ${formatMoney(totalPaidGross)}`,
      rightBoxX + 6,
      by
    );
    by += lineH;

    doc.text(
      `Refunded (incl. this): Rs ${formatMoney(refundedTillThis)}`,
      rightBoxX + 6,
      by
    );
    by += lineH;

    doc.text(
      `Balance: Rs ${formatMoney(balanceAfterThis)}`,
      rightBoxX + 6,
      by
    );

    const sigY = boxY + boxHeight + 40;
    const sigWidth = 160;

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

// ---------- GET /api/dashboard/summary ----------
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
      const dbb = b.refundDateTime
        ? new Date(b.refundDateTime)
        : new Date(0);
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
        "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
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

    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();

    y += 6;

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

// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
