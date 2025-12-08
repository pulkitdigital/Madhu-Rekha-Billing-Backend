// server.js
import express from "express";
import cors from "cors";
import "dotenv/config";
// import puppeteer from "puppeteer";
import PDFDocument from "pdfkit";
import path from "path";
import { fileURLToPath } from "url";
import { db } from "./firebaseClient.js";

const app = express();
const PORT = process.env.PORT || 4000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------- STATIC FILES (fonts, etc.) ----------
app.use("/resources", express.static(path.join(__dirname, "resources")));

// ---------- BASIC MIDDLEWARE ----------
app.use(cors());
app.use(express.json());

// ---------- HELPERS ----------
function computeStatus(total, paid) {
  if (!total || total <= 0) return "PENDING";
  if (paid >= total) return "PAID";
  if (paid > 0 && paid < total) return "PARTIAL";
  return "PENDING";
}

async function generateReceiptNumber() {
  const now = Date.now();
  const random = Math.floor(Math.random() * 1000);
  const suffix = `${String(now).slice(-6)}${String(random).padStart(3, "0")}`;
  return `RCP-${suffix}`;
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
//     total, paid, balance, doctorReg1, doctorReg2, status, createdAt }
//
// items:
//   { billId, description, qty, rate, amount }
//
// payments:
//   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
//     paymentDate, paymentTime, paymentDateTime, receiptNo }
//

// ---------- GET /api/bills (list) ----------
app.get("/api/bills", async (_req, res) => {
  try {
    // Newest first by invoiceNo (INV-YYYYMMDD-HHMMSS-NAME)
    const snapshot = await db
      .collection("bills")
      .orderBy("invoiceNo", "desc")
      .get();

    const mapped = snapshot.docs.map((doc) => {
      const b = doc.data();
      return {
        id: doc.id, // e.g. INV-20251206-130757-ROHIT
        invoiceNo: b.invoiceNo || doc.id,
        patientName: b.patientName || "",
        date: b.date || null,
        total: b.total || 0,
        paid: b.paid || 0,
        balance: b.balance || 0,
        status: b.status || "PENDING",
      };
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
    const balance = total - firstPay;

    const status = computeStatus(total, firstPay);

    // single ID used everywhere
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

// ---------- GET /api/bills/:id (detail + items + payments) ----------
app.get("/api/bills/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
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

    // Oldest first in history
    payments.sort((a, b) => {
      const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
      const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
      return da - dbb;
    });

    const primaryPayment = payments[0] || null;

    res.json({
      id,
      invoiceNo: bill.invoiceNo || id,
      patientName: bill.patientName || "",
      address: bill.address || "",
      age: bill.age || null,
      date: bill.date || null,
      subtotal: bill.subtotal || 0,
      adjust: bill.adjust || 0,
      total: bill.total || 0,
      paid: bill.paid || 0,
      balance: bill.balance || 0,
      status: bill.status || "PENDING",
      doctorReg1: bill.doctorReg1 || null,
      doctorReg2: bill.doctorReg2 || null,
      items,
      payments,
      paymentMode: primaryPayment?.mode || null,
      referenceNo: primaryPayment?.referenceNo || null,
      drawnOn: primaryPayment?.drawnOn || null,
      drawnAs: primaryPayment?.drawnAs || null,
    });
  } catch (err) {
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

    const newPaid = (bill.paid || 0) + numericAmount;
    const newBalance = (bill.total || 0) - newPaid;
    const newStatus = computeStatus(bill.total || 0, newPaid);

    const batch = db.batch();
    batch.set(paymentRef, paymentDoc);
    batch.update(billRef, {
      paid: newPaid,
      balance: newBalance,
      status: newStatus,
    });

    await batch.commit();

    res.status(201).json({
      id: paymentRef.id,
      ...paymentDoc,
    });
  } catch (err) {
    console.error("payment error:", err);
    res.status(500).json({ error: "Payment failed" });
  }
});

// ---------- GET /api/payments/:id (JSON for receipt page) ----------
app.get("/api/payments/:id", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    // 1) Load this payment
    const paymentRef = db.collection("payments").doc(id);
    const paymentSnap = await paymentRef.get();

    if (!paymentSnap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }

    const payment = paymentSnap.data();
    const billId = payment.billId;

    // 2) Load bill
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();

    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const bill = billSnap.data();
    const billTotal = Number(bill.total || 0);

    // 3) Load all payments for this bill
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
        return { id: doc.id, paymentDateTime, amount: d.amount };
      })
      .sort((a, b) => {
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
        const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
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

    // 4) Load bill items
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

    // 5) Response for ReceiptPrintPage
    res.json({
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
        paid: paidTillThis,          // up to THIS receipt
        balance: balanceAfterThis,   // after THIS receipt
        doctorReg1: bill.doctorReg1,
        doctorReg2: bill.doctorReg2,
        address: bill.address,
        age: bill.age,
        patientName: bill.patientName || "",
        items,
      },
    });
  } catch (err) {
    console.error("GET /api/payments/:id error:", err);
    res.status(500).json({ error: "Failed to load payment" });
  }
});

// ---------- PDF: Invoice ----------
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const browser = await puppeteer.launch({
//       headless: "new",
//       args: ["--no-sandbox", "--disable-setuid-sandbox"],
//     });
//     const page = await browser.newPage();

//     const url = `${FRONTEND_BASE}/print/invoice/${id}`;
//     console.log("Generating invoice PDF from URL:", url);

//     await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
//     await page.waitForSelector("[data-print-ready='1']", { timeout: 60000 });

//     const pdfBuffer = await page.pdf({
//       format: "A4",
//       printBackground: true,
//       preferCSSPageSize: false,
//       margin: {
//         top: "5mm",
//         bottom: "5mm",
//         left: "5mm",
//         right: "5mm",
//       },
//       pageRanges: "1",
//     });

//     await browser.close();

//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );
//     res.end(pdfBuffer);
//   } catch (err) {
//     console.error("invoice-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate invoice PDF" });
//     }
//   }
// });

// ---------- PDF: Invoice (PDFKit) ----------
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     // Load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // Load items
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const items = itemsSnap.docs.map((doc) => doc.data());

//     // Prepare response headers
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     // Create PDF
//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36, // 0.5 inch
//     });

//     doc.pipe(res);

//     // ---------- HEADER ----------
//     doc
//       .fontSize(18)
//       .text("MADHU REKHA EYE CARE", { align: "center" })
//       .moveDown(0.3);

//     doc
//       .fontSize(12)
//       .text("Invoice", { align: "center" })
//       .moveDown(1);

//     // ---------- BILL INFO ----------
//     doc
//       .fontSize(10)
//       .text(`Invoice No: ${bill.invoiceNo || id}`)
//       .text(`Date: ${bill.date || ""}`)
//       .moveDown(0.5);

//     // Patient details
//     doc
//       .fontSize(10)
//       .text(`Patient Name: ${bill.patientName || ""}`)
//       .text(`Age: ${bill.age ?? ""}`)
//       .text(`Address: ${bill.address || ""}`)
//       .moveDown(0.5);

//     if (bill.doctorReg1) doc.text(`Doctor Reg No 1: ${bill.doctorReg1}`);
//     if (bill.doctorReg2) doc.text(`Doctor Reg No 2: ${bill.doctorReg2}`);
//     doc.moveDown(1);

//     // ---------- ITEMS TABLE ----------
//     const tableTop = doc.y;

//     const colDescX = 36;
//     const colQtyX = 280;
//     const colRateX = 330;
//     const colAmountX = 400;

//     doc.fontSize(10).text("Description", colDescX, tableTop);
//     doc.text("Qty", colQtyX, tableTop);
//     doc.text("Rate", colRateX, tableTop);
//     doc.text("Amount", colAmountX, tableTop);

//     doc
//       .moveTo(36, tableTop + 12)
//       .lineTo(559, tableTop + 12)
//       .stroke();

//     let y = tableTop + 18;

//     items.forEach((item) => {
//       const qty = Number(item.qty || 0);
//       const rate = Number(item.rate || 0);
//       const amount = Number(item.amount || qty * rate);

//       doc.text(item.description || "", colDescX, y, { width: 230 });
//       doc.text(qty.toString(), colQtyX, y);
//       doc.text(rate.toFixed(2), colRateX, y);
//       doc.text(amount.toFixed(2), colAmountX, y);

//       y += 16;

//       // simple page break handling
//       if (y > 750) {
//         doc.addPage();
//         y = 36;
//       }
//     });

//     doc.moveDown(1.5);

//     // ---------- TOTALS ----------
//     const subtotal = Number(bill.subtotal || 0);
//     const adjust = Number(bill.adjust || 0);
//     const total = Number(bill.total || 0);
//     const paid = Number(bill.paid || 0);
//     const balance = Number(bill.balance || 0);

//     const totalsX = 330;

//     doc
//       .fontSize(10)
//       .text(`Subtotal:`, totalsX, doc.y)
//       .text(subtotal.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

//     doc
//       .text(`Adjustment:`, totalsX, doc.y)
//       .text(adjust.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

//     doc
//       .font("Helvetica-Bold")
//       .text(`Total:`, totalsX, doc.y)
//       .text(total.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

//     doc
//       .font("Helvetica")
//       .text(`Paid:`, totalsX, doc.y)
//       .text(paid.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

//     doc
//       .font("Helvetica-Bold")
//       .text(`Balance:`, totalsX, doc.y)
//       .text(balance.toFixed(2), totalsX + 100, doc.y - 12, {
//         align: "right",
//       });

//     doc.moveDown(2);

//     // ---------- FOOTER ----------
//     doc
//       .fontSize(9)
//       .font("Helvetica-Oblique")
//       .text("This is a computer-generated invoice.", {
//         align: "center",
//       });

//     doc.end();
//   } catch (err) {
//     console.error("invoice-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate invoice PDF" });
//     }
//   }
// });



// ---------- PDF: Invoice (PDFKit) ----------
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     // 1) Load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 2) Load items
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const items = itemsSnap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));

//     // 3) Load payments to get primary payment details
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", id)
//       .get();

//     const payments = paysSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         const paymentDateTime =
//           d.paymentDateTime ||
//           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//         return {
//           id: doc.id,
//           paymentDateTime,
//           amount: Number(d.amount || 0),
//           mode: d.mode || null,
//           referenceNo: d.referenceNo || null,
//           drawnOn: d.drawnOn || null,
//           drawnAs: d.drawnAs || null,
//         };
//       })
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//     const primaryPayment = payments[0] || null;

//     const paymentMode = primaryPayment?.mode || null;
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     // MONEY FIELDS
//     const subtotal = Number(bill.subtotal || 0);
//     const adjust = Number(bill.adjust || 0);
//     const total = Number(bill.total || 0);
//     const paid = Number(bill.paid || 0);
//     const balance = Number(bill.balance || 0);

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = bill.date || "";

//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";

//     // PDF RESPONSE HEADERS
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36, // ~0.5"
//     });

//     doc.pipe(res);

//     // ---------- HEADER AREA (logos + centre text) ----------
//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72; // margin*2
//     const leftX = 36;
//     const rightX = pageWidth - 36;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     // Left logo
//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {
//       // ignore if missing
//     }

//     // Right logo
//     try {
//       doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//     } catch (e) {
//       // ignore if missing
//     }

//     // Centre heading
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text("MADHUREKHA EYE CARE CENTRE", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(
//         "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
//         0,
//         y + 24,
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       )
//       .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
//         align: "center",
//         width: pageWidth,
//       });

//     // Horizontal line under header
//     y = 36 + 60;
//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     // ---------- DOCTORS ROW ----------
//     y += 4;
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text("Dr. Pradipta Kundu", 36, y);
//     doc.text("Dr. (Mrs.) Amita Kundu", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Reg. No.: ${bill.doctorReg1 || "________"}`, 36, y);
//     doc.text(
//       `Reg. No.: ${bill.doctorReg2 || "________"}`,
//       pageWidth / 2,
//       y,
//       {
//         align: "right",
//         width: usableWidth / 2,
//       }
//     );

//     y += 16;

//     // ---------- TITLE BAR ----------
//     doc
//       .rect(36, y, usableWidth, 18)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     // ---------- META + PATIENT INFO ----------
//     doc.font("Helvetica").fontSize(9);

//     // Row 1: Invoice + Date
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Row 2: Mr./Mrs. + Age
//     doc.text(`Mr./Mrs.: ${patientName}`, 36, y, {
//       width: usableWidth * 0.6,
//     });

//     if (ageText) {
//       doc.text(`Age: ${ageText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 12;

//     // Row 3: Address
//     doc.text(
//       `Address: ${bill.address || "________________________"}`,
//       36,
//       y,
//       {
//         width: usableWidth,
//       }
//     );

//     y += 18;

//     // ---------- ITEMS TABLE ----------
//     // Grid columns: [Sr. | Hrs/Qty | Service | Rate/Price | Adjust | Sub Total]
//     const tableLeft = 36;
//     const colSrW = 20;
//     const colQtyW = 40;
//     const colServiceW =  usableWidth - (colSrW + colQtyW + 60 + 60 + 60);
//     const colRateW = 60;
//     const colAdjW = 60;
//     const colSubW = 60;

//     const colSrX = tableLeft;
//     const colQtyX = colSrX + colSrW;
//     const colServiceX = colQtyX + colQtyW;
//     const colRateX = colServiceX + colServiceW;
//     const colAdjX = colRateX + colRateW;
//     const colSubX = colAdjX + colAdjW;

//     // Header row background (optional border only)
//     doc
//       .rect(tableLeft, y, usableWidth, 16)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3, { width: colSrW - 4 });
//     doc.text("Hrs / Qty", colQtyX + 2, y + 3, { width: colQtyW - 4 });
//     doc.text("Service", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//     doc.text("Rate / Price", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//     doc.text("Adjust", colAdjX + 2, y + 3, { width: colAdjW - 4, align: "right" });
//     doc.text("Sub Total", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });

//     y += 16;

//     doc.font("Helvetica").fontSize(9);

//     items.forEach((item, idx) => {
//       const qty = Number(item.qty || 0);
//       const rate = Number(item.rate || 0);
//       const amount = Number(item.amount || qty * rate);

//       const rowHeight = 14;

//       // row border
//       doc
//         .rect(tableLeft, y, usableWidth, rowHeight)
//         .stroke();

//       doc.text(String(idx + 1), colSrX + 2, y + 3, { width: colSrW - 4 });
//       doc.text(String(qty || ""), colQtyX + 2, y + 3, {
//         width: colQtyW - 4,
//         align: "left",
//       });
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(formatMoney(rate), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text("0.00", colAdjX + 2, y + 3, {
//         width: colAdjW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(amount), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       y += rowHeight;

//       // simple page break
//       if (y > doc.page.height - 160) {
//         doc.addPage();
//         y = 36;
//       }
//     });

//     y += 14;

//     // ---------- SUMMARY BOX (right side) ----------
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     doc
//       .rect(boxX, boxY, boxWidth, lineH * 4 + 4)
//       .stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Sub Total
//     doc.text("Sub Total", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(subtotal)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // Adjust
//     doc.text("Adjust", boxX + 6, boxY + 2 + lineH);
//     doc.text(`Rs ${formatMoney(adjust)}`, boxX, boxY + 2 + lineH, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // Tax (always 0)
//     doc.text("Tax", boxX + 6, boxY + 2 + lineH * 2);
//     doc.text("Rs 0.00", boxX, boxY + 2 + lineH * 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // Total Due (bold)
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2 + lineH * 3);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2 + lineH * 3, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.font("Helvetica");
//     y = boxY + lineH * 4 + 16;

//     // ---------- Paid / Balance Row ----------
//     doc.fontSize(9);
//     doc.text(`Amount Paid: Rs ${formatMoney(paid)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 22;

//     // ---------- Bottom Receipt Text ----------
//     const receiptTextWidth = usableWidth;
//     doc.fontSize(9);

//     // "Received with thanks..." line
//     let line = `Received with thanks from Shri/Smt./M/s ${patientName} the sum of Rupees Rs ${formatMoney(
//       paid
//     )} dated ${dateText} by ${paymentMode || "________"} / Bank / Transfer / Cheque No. / UPI`;
//     if (referenceNo) {
//       line += ` (${referenceNo})`;
//     }
//     line += ".";

//     doc.text(line, 36, y, { width: receiptTextWidth });

//     y = doc.y + 8;

//     // "Drawn on..." line
//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";
//     doc.text(
//       `Drawn on ${drawnOnText} (Subject to realization) as ${drawnAsText}.`,
//       36,
//       y,
//       { width: receiptTextWidth }
//     );

//     y = doc.y + 6;

//     doc.fontSize(8).text(
//       "* Dispute if any Subject to Jamshedpur Jurisdiction",
//       36,
//       y,
//       { width: receiptTextWidth }
//     );

//     y = doc.y + 30;

//     // ---------- Signatures ----------
//     const sigWidth = 150;
//     const sigY = y;

//     // Left signature
//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();

//     doc.fontSize(8).text("Patient / Representative", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     // Right signature
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();

//     doc
//       .fontSize(8)
//       .text(
//         "For Madhurekha Eye Care Centre",
//         rightSigX,
//         sigY + 4,
//         {
//           width: sigWidth,
//           align: "center",
//         }
//       );

//     doc.end();
//   } catch (err) {
//     console.error("invoice-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate invoice PDF" });
//     }
//   }
// });


// ---------- PDF: Invoice (PDFKit – A4 full page) ----------
app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    // 1) Load bill
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();

    // 2) Load items
    const itemsSnap = await db
      .collection("items")
      .where("billId", "==", id)
      .get();

    const items = itemsSnap.docs.map((doc) => ({
      id: doc.id,
      ...doc.data(),
    }));

    // 3) Load payments to get primary payment details
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

    const paymentMode = primaryPayment?.mode || "Cash";
    const referenceNo = primaryPayment?.referenceNo || null;
    const drawnOn = primaryPayment?.drawnOn || null;
    const drawnAs = primaryPayment?.drawnAs || null;

    // MONEY FIELDS
    const subtotal = Number(bill.subtotal || 0);
    const adjust = Number(bill.adjust || 0);
    const total = Number(bill.total || 0);
    const paid = Number(bill.paid || 0);
    const balance = Number(bill.balance || 0);

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const invoiceNo = bill.invoiceNo || id;
    const dateText = bill.date || "";

    const patientName = bill.patientName || "";
    const ageText =
      bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";

    // PDF HEADERS
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="invoice-${id}.pdf"`
    );

    const doc = new PDFDocument({
      size: "A4",
      margin: 36, // 0.5"
    });

    doc.pipe(res);

    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72; // left+right margin
    let y = 36;

    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

    // ---------- HEADER (logos + centre text) ----------
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

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();

    y += 4;

    // ---------- DOCTORS ROW ----------
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

    // ---------- TITLE BAR ----------
    doc
      .rect(36, y, usableWidth, 18)
      .stroke();

    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
        align: "center",
        width: usableWidth,
      });

    y += 26;

    // ---------- META + PATIENT INFO ----------
    doc.font("Helvetica").fontSize(9);

    // row 1
    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

    // row 2
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

    // row 3
    doc.text(
      `Address: ${bill.address || "________________________"}`,
      36,
      y,
      { width: usableWidth }
    );

    y += 20;

    // ---------- ITEMS TABLE ----------
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

    // header row box
    doc
      .rect(tableLeft, y, usableWidth, 16)
      .stroke();

    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Sr.", colSrX + 2, y + 3);
    doc.text("Hrs / Qty", colQtyX + 2, y + 3);
    doc.text("Service", colServiceX + 2, y + 3, { width: colServiceW - 4 });
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

      doc
        .rect(tableLeft, y, usableWidth, rowHeight)
        .stroke();

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

    // ---------- SUMMARY BOX (right) ----------
    const boxWidth = 180;
    const boxX = pageWidth - 36 - boxWidth;
    const boxY = y;
    const lineH = 12;

    doc
      .rect(boxX, boxY, boxWidth, lineH * 4 + 4)
      .stroke();

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

    doc.font("Helvetica-Bold");
    doc.text("Total Due", boxX + 6, boxY + 2 + lineH * 3);
    doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2 + lineH * 3, {
      width: boxWidth - 6,
      align: "right",
    });

    doc.font("Helvetica");
    y = boxY + lineH * 4 + 20;

    // ---------- Paid / Balance ----------
    doc.fontSize(9);
    doc.text(`Amount Paid: Rs ${formatMoney(paid)}`, 36, y);
    doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 22;

    // ---------- Bottom receipt text ----------
    const fullWidth = usableWidth;

    let line = `Received with thanks from Shri/Smt./M/s ${patientName} the sum of Rupees Rs ${formatMoney(
      paid
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

    doc.fontSize(8).text(
      "* Dispute if any Subject to Jamshedpur Jurisdiction",
      36,
      y,
      { width: fullWidth }
    );

    y = doc.y + 30;

    // ---------- Signatures ----------
    const sigWidth = 160;
    const sigY = y;

    // left
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

    // right
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




// ---------- PDF: Receipt ----------
// app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   try {
//     const paymentRef = db.collection("payments").doc(id);
//     const paymentSnap = await paymentRef.get();
//     if (!paymentSnap.exists) {
//       return res.status(404).json({ error: "Payment not found" });
//     }

//     const browser = await puppeteer.launch({
//       headless: "new",
//       args: ["--no-sandbox", "--disable-setuid-sandbox"],
//     });
//     const page = await browser.newPage();

//     const url = `${FRONTEND_BASE}/print/receipt/${id}`;
//     console.log("Generating receipt PDF from URL:", url);

//     await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
//     await page.waitForSelector("[data-print-ready='1']", { timeout: 60000 });

//     const pdfBuffer = await page.pdf({
//       width: "210mm",
//       height: "148mm",
//       printBackground: true,
//       preferCSSPageSize: false,
//       margin: {
//         top: "3mm",
//         bottom: "3mm",
//         left: "3mm",
//         right: "3mm",
//       },
//       pageRanges: "1",
//       scale: 0.95,
//     });

//     await browser.close();

//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="receipt-${id}.pdf"`
//     );
//     res.end(pdfBuffer);
//   } catch (err) {
//     console.error("receipt-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate receipt PDF" });
//     }
//   }
// });

// ---------- PDF: Receipt (PDFKit) ----------
// app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   try {
//     // 1) Load this payment
//     const paymentRef = db.collection("payments").doc(id);
//     const paymentSnap = await paymentRef.get();
//     if (!paymentSnap.exists) {
//       return res.status(404).json({ error: "Payment not found" });
//     }
//     const payment = paymentSnap.data();
//     const billId = payment.billId;

//     // 2) Load bill
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 3) Load all payments for this bill to compute cumulative paid & balance
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const billTotal = Number(bill.total || 0);

//     const allPayments = paysSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         const paymentDateTime =
//           d.paymentDateTime ||
//           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//         return {
//           id: doc.id,
//           paymentDateTime,
//           amount: Number(d.amount || 0),
//         };
//       })
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     let cumulativePaid = 0;
//     let paidTillThis = 0;
//     let balanceAfterThis = billTotal;

//     for (const p of allPayments) {
//       cumulativePaid += p.amount;
//       if (p.id === id) {
//         paidTillThis = cumulativePaid;
//         balanceAfterThis = billTotal - paidTillThis;
//         break;
//       }
//     }

//     const receiptNo =
//       payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

//     // ---------- PDF OUTPUT ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="receipt-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: [595.28, 420], // approx A5 landscape in points
//       margin: 28,
//     });

//     doc.pipe(res);

//     // HEADER
//     doc
//       .fontSize(16)
//       .text("MADHU REKHA EYE CARE", { align: "center" })
//       .moveDown(0.3);

//     doc.fontSize(12).text("Payment Receipt", { align: "center" }).moveDown(1);

//     // RECEIPT INFO
//     doc
//       .fontSize(10)
//       .text(`Receipt No: ${receiptNo}`)
//       .text(`Receipt Date: ${payment.paymentDate || ""}`)
//       .moveDown(0.5);

//     // PATIENT / BILL INFO
//     doc
//       .fontSize(10)
//       .text(`Bill No: ${bill.invoiceNo || billId}`)
//       .text(`Bill Date: ${bill.date || ""}`)
//       .moveDown(0.5);

//     doc
//       .text(`Patient Name: ${bill.patientName || ""}`)
//       .text(`Age: ${bill.age ?? ""}`)
//       .text(`Address: ${bill.address || ""}`)
//       .moveDown(0.5);

//     if (bill.doctorReg1) doc.text(`Doctor Reg No 1: ${bill.doctorReg1}`);
//     if (bill.doctorReg2) doc.text(`Doctor Reg No 2: ${bill.doctorReg2}`);
//     doc.moveDown(1);

//     // PAYMENT DETAILS
//     doc
//       .fontSize(10)
//       .text(`Amount Received: Rs. ${Number(payment.amount || 0).toFixed(2)}`)
//       .text(`Mode: ${payment.mode || "Cash"}`);

//     if (payment.referenceNo) {
//       doc.text(`Reference No: ${payment.referenceNo}`);
//     }

//     if (payment.drawnOn) {
//       doc.text(`Drawn On: ${payment.drawnOn}`);
//     }

//     if (payment.drawnAs) {
//       doc.text(`Drawn As: ${payment.drawnAs}`);
//     }

//     doc.moveDown(1);

//     // BILL SUMMARY AFTER THIS PAYMENT
//     doc
//       .fontSize(10)
//       .text(`Bill Total: Rs. ${billTotal.toFixed(2)}`)
//       .text(`Total Paid (till this receipt): Rs. ${paidTillThis.toFixed(2)}`)
//       .font("Helvetica-Bold")
//       .text(
//         `Balance After This Payment: Rs. ${balanceAfterThis.toFixed(2)}`
//       )
//       .font("Helvetica")
//       .moveDown(2);

//     // FOOTER
//     doc
//       .fontSize(9)
//       .font("Helvetica-Oblique")
//       .text("This is a computer-generated receipt.", {
//         align: "center",
//       });

//     doc.end();
//   } catch (err) {
//     console.error("receipt-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate receipt PDF" });
//     }
//   }
// });




// ---------- PDF: Receipt (PDFKit) ----------
// app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   try {
//     // 1) Load this payment
//     const paymentRef = db.collection("payments").doc(id);
//     const paymentSnap = await paymentRef.get();
//     if (!paymentSnap.exists) {
//       return res.status(404).json({ error: "Payment not found" });
//     }
//     const payment = paymentSnap.data();
//     const billId = payment.billId;

//     // 2) Load bill
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 3) Load all payments for this bill to compute cumulative paid & balance
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const billTotal = Number(bill.total || 0);

//     const allPayments = paysSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         const paymentDateTime =
//           d.paymentDateTime ||
//           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//         return {
//           id: doc.id,
//           paymentDateTime,
//           amount: Number(d.amount || 0),
//         };
//       })
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//     let cumulativePaid = 0;
//     let paidTillThis = 0;
//     let balanceAfterThis = billTotal;

//     for (const p of allPayments) {
//       cumulativePaid += p.amount;
//       if (p.id === id) {
//         paidTillThis = cumulativePaid;
//         balanceAfterThis = billTotal - paidTillThis;
//         break;
//       }
//     }

//     const receiptNo =
//       payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const patientName = bill.patientName || "";
//     const drawnOn = payment.drawnOn || null;
//     const drawnAs = payment.drawnAs || null;
//     const mode = payment.mode || "Cash";
//     const referenceNo = payment.referenceNo || null;

//     // ---------- PDF OUTPUT ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="receipt-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",   // A4 PAGE
//       margin: 36,   // thoda sa bada margin
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72; // 2 * margin
//     let y = 36; // start near top (half-page ke andar hi sab khatam ho jayega)

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     // ---------- HEADER (logos + centre text) ----------
//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 36, height: 36 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 36, y, { width: 36, height: 36 });
//     } catch (e) {}

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(12)
//       .text("MADHUREKHA EYE CARE CENTRE", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(
//         "SONARI: E-501, Sonari East Layout, Near Subzi Sangh, Kali Puja Maidan, Jamshedpur - 831011",
//         0,
//         y + 20,
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       )
//       .text("PAN : ABFFM3115J   |   Reg. No: 2035700023", {
//         align: "center",
//         width: pageWidth,
//       });

//     y += 44;

//     // line under header
//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 6;

//     // ---------- TITLE BAR ----------
//     doc
//       .rect(36, y, usableWidth, 16)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("PAYMENT RECEIPT", 36, y + 3, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 22;

//     // ---------- META ROW ----------
//     doc.font("Helvetica").fontSize(9);

//     doc.text(`Receipt No.: ${receiptNo}`, 36, y);
//     doc.text(`Date: ${payment.paymentDate || ""}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 14;

//     // ---------- MAIN CONTENT: LEFT TEXT + RIGHT BILL SUMMARY BOX ----------
//     const leftBlockX = 36;
//     const leftBlockWidth = usableWidth - 190; // leave room for summary box
//     const rightBoxWidth = 170;
//     const rightBoxX = pageWidth - 36 - rightBoxWidth;

//     const textYStart = y;
//     doc.fontSize(9);

//     // "Received with thanks..." line
//     let line = `Received with thanks from Shri/Smt./M/s ${patientName} the sum of Rupees Rs ${formatMoney(
//       payment.amount
//     )} by ${mode} / Bank / Transfer / Cheque No. / UPI`;
//     if (referenceNo) {
//       line += ` (${referenceNo})`;
//     }
//     line += ".";

//     doc.text(line, leftBlockX, y, {
//       width: leftBlockWidth,
//     });

//     y = doc.y + 6;

//     // Drawn on / as
//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     doc.text(
//       `Drawn on ${drawnOnText} (Subject to realization) as ${drawnAsText} towards consultation / services.`,
//       leftBlockX,
//       y,
//       {
//         width: leftBlockWidth,
//       }
//     );

//     y = doc.y + 6;

//     doc.fontSize(8).text(
//       "* Dispute if any Subject to Jamshedpur Jurisdiction",
//       leftBlockX,
//       y,
//       {
//         width: leftBlockWidth,
//       }
//     );

//     // Right: Bill summary box
//     const boxY = textYStart;
//     const lineH = 11;
//     const boxHeight = lineH * 6 + 6;

//     doc
//       .rect(rightBoxX, boxY, rightBoxWidth, boxHeight)
//       .stroke();

//     let by = boxY;

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(9)
//       .text("Bill Summary", rightBoxX + 6, by + 2);

//     by += lineH + 2;
//     doc.font("Helvetica").fontSize(9);

//     const billNoText = bill.invoiceNo || billId;
//     doc.text(`Bill No.: ${billNoText}`, rightBoxX + 6, by);
//     by += lineH;

//     doc.text(`Bill Date: ${bill.date || ""}`, rightBoxX + 6, by);
//     by += lineH;

//     doc.text(
//       `Bill Total: Rs ${formatMoney(billTotal)}`,
//       rightBoxX + 6,
//       by
//     );
//     by += lineH;

//     doc.text(
//       `Paid (incl. this): Rs ${formatMoney(paidTillThis)}`,
//       rightBoxX + 6,
//       by
//     );
//     by += lineH;

//     doc.text(
//       `Balance: Rs ${formatMoney(balanceAfterThis)}`,
//       rightBoxX + 6,
//       by
//     );

//     // ---------- SIGNATURES (still top half) ----------
//     const sigY = boxY + boxHeight + 32;
//     const sigWidth = 150;

//     // Left signature
//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text("Patient / Representative", 36, sigY + 3, {
//         width: sigWidth,
//         align: "center",
//       });

//     // Right signature
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text("For Madhurekha Eye Care Centre", rightSigX, sigY + 3, {
//         width: sigWidth,
//         align: "center",
//       });

//     // Everything is in roughly upper half of A4; rest blank.

//     doc.end();
//   } catch (err) {
//     console.error("receipt-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate receipt PDF" });
//     }
//   }
// });



// ---------- PDF: Receipt (PDFKit – A4 half page) ----------
app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    // 1) Load this payment
    const paymentRef = db.collection("payments").doc(id);
    const paymentSnap = await paymentRef.get();
    if (!paymentSnap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }
    const payment = paymentSnap.data();
    const billId = payment.billId;

    // 2) Load bill
    const billRef = db.collection("bills").doc(billId);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }
    const bill = billSnap.data();

    // 3) Load all payments for this bill
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

    const receiptNo =
      payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

    function formatMoney(v) {
      return Number(v || 0).toFixed(2);
    }

    const patientName = bill.patientName || "";
    const drawnOn = payment.drawnOn || null;
    const drawnAs = payment.drawnAs || null;
    const mode = payment.mode || "Cash";
    const referenceNo = payment.referenceNo || null;

    // ---------- PDF OUTPUT ----------
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

    // ---------- HEADER ----------
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

    doc
      .moveTo(36, y)
      .lineTo(pageWidth - 36, y)
      .stroke();

    y += 6;

    // ---------- TITLE ----------
    doc
      .rect(36, y, usableWidth, 16)
      .stroke();

    doc
      .font("Helvetica-Bold")
      .fontSize(10)
      .text("PAYMENT RECEIPT", 36, y + 3, {
        align: "center",
        width: usableWidth,
      });

    y += 24;

    // ---------- META ----------
    doc.font("Helvetica").fontSize(9);
    doc.text(`Receipt No.: ${receiptNo}`, 36, y);
    doc.text(`Date: ${payment.paymentDate || ""}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 16;

    // ---------- MAIN CONTENT ----------
    const leftBlockX = 36;
    const leftBlockWidth = usableWidth - 190;
    const rightBoxWidth = 170;
    const rightBoxX = pageWidth - 36 - rightBoxWidth;

    const textYStart = y;

    doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, leftBlockX, y, {
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

    // ---------- BILL SUMMARY BOX (right) ----------
    const boxY = textYStart;
    const lineH = 12;
    const boxHeight = lineH * 6 + 6;

    doc
      .rect(rightBoxX, boxY, rightBoxWidth, boxHeight)
      .stroke();

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

    // ---------- SIGNATURES (still top half) ----------
    const sigY = boxY + boxHeight + 40;
    const sigWidth = 160;

    // left
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

    // right
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

    // bottom half blank = clean half-page receipt

    doc.end();
  } catch (err) {
    console.error("receipt-html-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate receipt PDF" });
    }
  }
});




// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
