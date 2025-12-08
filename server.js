// // // server.js
// // import express from "express";
// // import cors from "cors";
// // import "dotenv/config";
// // // import puppeteer from "puppeteer";
// // import PDFDocument from "pdfkit";
// // import path from "path";
// // import { fileURLToPath } from "url";
// // import { db } from "./firebaseClient.js";

// // const app = express();
// // const PORT = process.env.PORT || 4000;

// // const __filename = fileURLToPath(import.meta.url);
// // const __dirname = path.dirname(__filename);

// // // ---------- STATIC FILES (fonts, etc.) ----------
// // app.use("/resources", express.static(path.join(__dirname, "resources")));

// // // ---------- BASIC MIDDLEWARE ----------
// // app.use(cors());
// // app.use(express.json());

// // // ---------- HELPERS ----------
// // function computeStatus(total, paid) {
// //   if (!total || total <= 0) return "PENDING";
// //   if (paid >= total) return "PAID";
// //   if (paid > 0 && paid < total) return "PARTIAL";
// //   return "PENDING";
// // }

// // async function generateReceiptNumber() {
// //   const now = Date.now();
// //   const random = Math.floor(Math.random() * 1000);
// //   const suffix = `${String(now).slice(-6)}${String(random).padStart(3, "0")}`;
// //   return `RCP-${suffix}`;
// // }

// // function formatDateYYYYMMDD(dateStrOrDate) {
// //   const d = dateStrOrDate ? new Date(dateStrOrDate) : new Date();
// //   const yyyy = d.getFullYear();
// //   const mm = String(d.getMonth() + 1).padStart(2, "0");
// //   const dd = String(d.getDate()).padStart(2, "0");
// //   return `${yyyy}${mm}${dd}`;
// // }

// // // Turn "Rohit Sharma" => "ROHITSHAR"
// // function makeNameSlug(name) {
// //   if (!name) return "NONAME";
// //   const slug = name
// //     .toUpperCase()
// //     .replace(/[^A-Z]/g, "")
// //     .slice(0, 8);
// //   return slug || "NONAME";
// // }

// // // Bill id + invoiceNo: INV-YYYYMMDD-HHMMSS-NAME
// // function generateBillId(patientName, billDateInput) {
// //   const datePart = formatDateYYYYMMDD(billDateInput);
// //   const now = new Date();
// //   const timePart = `${String(now.getHours()).padStart(2, "0")}${String(
// //     now.getMinutes()
// //   ).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
// //   const nameSlug = makeNameSlug(patientName);
// //   return `INV-${datePart}-${timePart}-${nameSlug}`;
// // }

// // // FRONTEND base URL (React app)
// // const FRONTEND_BASE =
// //   process.env.FRONTEND_BASE_URL ||
// //   (process.env.NODE_ENV === "production"
// //     ? "https://madhu-rekha-billing-software.vercel.app"
// //     : "http://localhost:5173");

// // // ---------- HEALTH CHECK ----------
// // app.get("/", (_req, res) => {
// //   res.send("Backend OK");
// // });

// // //
// // // FIRESTORE SCHEMA:
// // //
// // // bills:
// // //   { patientName, address, age, date, invoiceNo, subtotal, adjust,
// // //     total, paid, balance, doctorReg1, doctorReg2, status, createdAt }
// // //
// // // items:
// // //   { billId, description, qty, rate, amount }
// // //
// // // payments:
// // //   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
// // //     paymentDate, paymentTime, paymentDateTime, receiptNo }
// // //

// // // ---------- GET /api/bills (list) ----------
// // app.get("/api/bills", async (_req, res) => {
// //   try {
// //     // Newest first by invoiceNo (INV-YYYYMMDD-HHMMSS-NAME)
// //     const snapshot = await db
// //       .collection("bills")
// //       .orderBy("invoiceNo", "desc")
// //       .get();

// //     const mapped = snapshot.docs.map((doc) => {
// //       const b = doc.data();
// //       return {
// //         id: doc.id, // e.g. INV-20251206-130757-ROHIT
// //         invoiceNo: b.invoiceNo || doc.id,
// //         patientName: b.patientName || "",
// //         date: b.date || null,
// //         total: b.total || 0,
// //         paid: b.paid || 0,
// //         balance: b.balance || 0,
// //         status: b.status || "PENDING",
// //       };
// //     });

// //     res.json(mapped);
// //   } catch (err) {
// //     console.error("GET /api/bills error:", err);
// //     res.status(500).json({ error: "Failed to fetch bills" });
// //   }
// // });

// // // ---------- POST /api/bills (create bill + optional first payment) ----------
// // app.post("/api/bills", async (req, res) => {
// //   try {
// //     const {
// //       patientName,
// //       address,
// //       age,
// //       date,
// //       doctorReg1,
// //       doctorReg2,
// //       adjust,
// //       pay,
// //       paymentMode,
// //       referenceNo,
// //       drawnOn,
// //       drawnAs,
// //       services,
// //     } = req.body;

// //     const jsDate = date || new Date().toISOString().slice(0, 10);

// //     const itemsData = (services || []).map((s) => {
// //       const qty = Number(s.qty) || 0;
// //       const rate = Number(s.rate) || 0;
// //       const amount = qty * rate;
// //       return {
// //         description: s.description || "",
// //         qty,
// //         rate,
// //         amount,
// //       };
// //     });

// //     const subtotal = itemsData.reduce((sum, it) => sum + Number(it.amount), 0);
// //     const adj = Number(adjust) || 0;
// //     const total = subtotal + adj;
// //     const firstPay = Number(pay) || 0;
// //     const balance = total - firstPay;

// //     const status = computeStatus(total, firstPay);

// //     // single ID used everywhere
// //     const billId = generateBillId(patientName, jsDate);
// //     const createdAt = new Date().toISOString();

// //     const billRef = db.collection("bills").doc(billId);
// //     const batch = db.batch();

// //     // 1) Bill
// //     batch.set(billRef, {
// //       patientName: patientName || "",
// //       address: address || "",
// //       age: age ? Number(age) : null,
// //       date: jsDate,
// //       invoiceNo: billId,
// //       doctorReg1: doctorReg1 || null,
// //       doctorReg2: doctorReg2 || null,
// //       subtotal,
// //       adjust: adj,
// //       total,
// //       paid: firstPay,
// //       balance,
// //       status,
// //       createdAt,
// //     });

// //     // 2) Items
// //     itemsData.forEach((item) => {
// //       const itemRef = db.collection("items").doc();
// //       batch.set(itemRef, {
// //         billId,
// //         ...item,
// //       });
// //     });

// //     // 3) Optional first payment
// //     let paymentDoc = null;
// //     let receiptDoc = null;

// //     if (firstPay > 0) {
// //       const receiptNo = await generateReceiptNumber();
// //       const paymentRef = db.collection("payments").doc();
// //       const now = new Date();
// //       const paymentDate = jsDate;
// //       const paymentTime = now.toTimeString().slice(0, 5);
// //       const paymentDateTime = now.toISOString();

// //       paymentDoc = {
// //         billId,
// //         amount: firstPay,
// //         mode: paymentMode || "Cash",
// //         referenceNo: referenceNo || null,
// //         drawnOn: drawnOn || null,
// //         drawnAs: drawnAs || null,
// //         paymentDate,
// //         paymentTime,
// //         paymentDateTime,
// //         receiptNo,
// //       };
// //       batch.set(paymentRef, paymentDoc);
// //       receiptDoc = { receiptNo };
// //     }

// //     await batch.commit();

// //     res.json({
// //       bill: {
// //         id: billId,
// //         invoiceNo: billId,
// //         patientName: patientName || "",
// //         address: address || "",
// //         age: age ? Number(age) : null,
// //         date: jsDate,
// //         doctorReg1: doctorReg1 || null,
// //         doctorReg2: doctorReg2 || null,
// //         subtotal,
// //         adjust: adj,
// //         total,
// //         paid: firstPay,
// //         balance,
// //         status,
// //         items: itemsData,
// //         paymentMode: paymentDoc?.mode || null,
// //         referenceNo: paymentDoc?.referenceNo || null,
// //         drawnOn: paymentDoc?.drawnOn || null,
// //         drawnAs: paymentDoc?.drawnAs || null,
// //       },
// //       payment: paymentDoc,
// //       receipt: receiptDoc,
// //     });
// //   } catch (err) {
// //     console.error("POST /api/bills error:", err);
// //     res.status(500).json({ error: "Failed to create bill" });
// //   }
// // });

// // // ---------- GET /api/bills/:id (detail + items + payments) ----------
// // app.get("/api/bills/:id", async (req, res) => {
// //   const id = req.params.id;
// //   if (!id) return res.status(400).json({ error: "Invalid bill id" });

// //   try {
// //     const billRef = db.collection("bills").doc(id);
// //     const billSnap = await billRef.get();
// //     if (!billSnap.exists) {
// //       return res.status(404).json({ error: "Bill not found" });
// //     }

// //     const bill = billSnap.data();

// //     // Items
// //     const itemsSnap = await db
// //       .collection("items")
// //       .where("billId", "==", id)
// //       .get();

// //     const items = itemsSnap.docs.map((doc) => ({
// //       id: doc.id,
// //       ...doc.data(),
// //     }));

// //     // Payments
// //     const paysSnap = await db
// //       .collection("payments")
// //       .where("billId", "==", id)
// //       .get();

// //     let payments = paysSnap.docs.map((doc) => {
// //       const d = doc.data();
// //       const paymentDateTime =
// //         d.paymentDateTime ||
// //         (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);

// //       return {
// //         id: doc.id,
// //         amount: Number(d.amount || 0),
// //         mode: d.mode || "",
// //         referenceNo: d.referenceNo || null,
// //         receiptNo: d.receiptNo || null,
// //         date: d.paymentDate || null,
// //         time: d.paymentTime || null,
// //         paymentDateTime,
// //         drawnOn: d.drawnOn || null,
// //         drawnAs: d.drawnAs || null,
// //       };
// //     });

// //     // Oldest first in history
// //     payments.sort((a, b) => {
// //       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
// //       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
// //       return da - dbb;
// //     });

// //     const primaryPayment = payments[0] || null;

// //     res.json({
// //       id,
// //       invoiceNo: bill.invoiceNo || id,
// //       patientName: bill.patientName || "",
// //       address: bill.address || "",
// //       age: bill.age || null,
// //       date: bill.date || null,
// //       subtotal: bill.subtotal || 0,
// //       adjust: bill.adjust || 0,
// //       total: bill.total || 0,
// //       paid: bill.paid || 0,
// //       balance: bill.balance || 0,
// //       status: bill.status || "PENDING",
// //       doctorReg1: bill.doctorReg1 || null,
// //       doctorReg2: bill.doctorReg2 || null,
// //       items,
// //       payments,
// //       paymentMode: primaryPayment?.mode || null,
// //       referenceNo: primaryPayment?.referenceNo || null,
// //       drawnOn: primaryPayment?.drawnOn || null,
// //       drawnAs: primaryPayment?.drawnAs || null,
// //     });
// //   } catch (err) {
// //     console.error("bill detail error:", err);
// //     res.status(500).json({ error: "Failed to load bill" });
// //   }
// // });

// // // ---------- POST /api/bills/:id/payments (add partial payment) ----------
// // app.post("/api/bills/:id/payments", async (req, res) => {
// //   const billId = req.params.id;
// //   if (!billId) {
// //     return res.status(400).json({ error: "Invalid bill id" });
// //   }

// //   const { amount, mode, referenceNo, drawnOn, drawnAs } = req.body;
// //   const numericAmount = Number(amount);

// //   if (!numericAmount || numericAmount <= 0) {
// //     return res.status(400).json({ error: "Amount must be > 0" });
// //   }

// //   try {
// //     const billRef = db.collection("bills").doc(billId);
// //     const billSnap = await billRef.get();

// //     if (!billSnap.exists) {
// //       return res.status(404).json({ error: "Bill not found" });
// //     }

// //     const bill = billSnap.data();

// //     const now = new Date();
// //     const paymentDate = now.toISOString().slice(0, 10);
// //     const paymentTime = now.toTimeString().slice(0, 5);
// //     const paymentDateTime = now.toISOString();
// //     const receiptNo = await generateReceiptNumber();

// //     const paymentRef = db.collection("payments").doc();
// //     const paymentDoc = {
// //       billId,
// //       amount: numericAmount,
// //       mode: mode || "Cash",
// //       referenceNo: referenceNo || null,
// //       drawnOn: drawnOn || null,
// //       drawnAs: drawnAs || null,
// //       paymentDate,
// //       paymentTime,
// //       paymentDateTime,
// //       receiptNo,
// //     };

// //     const newPaid = (bill.paid || 0) + numericAmount;
// //     const newBalance = (bill.total || 0) - newPaid;
// //     const newStatus = computeStatus(bill.total || 0, newPaid);

// //     const batch = db.batch();
// //     batch.set(paymentRef, paymentDoc);
// //     batch.update(billRef, {
// //       paid: newPaid,
// //       balance: newBalance,
// //       status: newStatus,
// //     });

// //     await batch.commit();

// //     res.status(201).json({
// //       id: paymentRef.id,
// //       ...paymentDoc,
// //     });
// //   } catch (err) {
// //     console.error("payment error:", err);
// //     res.status(500).json({ error: "Payment failed" });
// //   }
// // });

// // // ---------- GET /api/payments/:id (JSON for receipt page) ----------
// // app.get("/api/payments/:id", async (req, res) => {
// //   const id = req.params.id;
// //   if (!id) return res.status(400).json({ error: "Invalid payment id" });

// //   try {
// //     // 1) Load this payment
// //     const paymentRef = db.collection("payments").doc(id);
// //     const paymentSnap = await paymentRef.get();

// //     if (!paymentSnap.exists) {
// //       return res.status(404).json({ error: "Payment not found" });
// //     }

// //     const payment = paymentSnap.data();
// //     const billId = payment.billId;

// //     // 2) Load bill
// //     const billRef = db.collection("bills").doc(billId);
// //     const billSnap = await billRef.get();

// //     if (!billSnap.exists) {
// //       return res.status(404).json({ error: "Bill not found" });
// //     }

// //     const bill = billSnap.data();
// //     const billTotal = Number(bill.total || 0);

// //     // 3) Load all payments for this bill
// //     const paysSnap = await db
// //       .collection("payments")
// //       .where("billId", "==", billId)
// //       .get();

// //     const allPayments = paysSnap.docs
// //       .map((doc) => {
// //         const d = doc.data();
// //         const paymentDateTime =
// //           d.paymentDateTime ||
// //           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
// //         return { id: doc.id, paymentDateTime, amount: d.amount };
// //       })
// //       .sort((a, b) => {
// //         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
// //         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
// //         return da - dbb;
// //       });

// //     let cumulativePaid = 0;
// //     let paidTillThis = 0;
// //     let balanceAfterThis = billTotal;

// //     for (const p of allPayments) {
// //       cumulativePaid += Number(p.amount || 0);
// //       if (p.id === id) {
// //         paidTillThis = cumulativePaid;
// //         balanceAfterThis = billTotal - paidTillThis;
// //         break;
// //       }
// //     }

// //     // 4) Load bill items
// //     const itemsSnap = await db
// //       .collection("items")
// //       .where("billId", "==", billId)
// //       .get();

// //     const items = itemsSnap.docs.map((doc) => {
// //       const d = doc.data();
// //       return {
// //         id: doc.id,
// //         description: d.description,
// //         qty: Number(d.qty),
// //         rate: Number(d.rate),
// //         amount: Number(d.amount),
// //       };
// //     });

// //     // 5) Response for ReceiptPrintPage
// //     res.json({
// //       id,
// //       amount: Number(payment.amount),
// //       mode: payment.mode,
// //       referenceNo: payment.referenceNo,
// //       drawnOn: payment.drawnOn,
// //       drawnAs: payment.drawnAs,
// //       paymentDate: payment.paymentDate,
// //       receiptNo: payment.receiptNo || `R-${String(id).padStart(4, "0")}`,
// //       bill: {
// //         id: billId,
// //         date: bill.date,
// //         subtotal: Number(bill.subtotal),
// //         adjust: Number(bill.adjust),
// //         total: billTotal,
// //         paid: paidTillThis,          // up to THIS receipt
// //         balance: balanceAfterThis,   // after THIS receipt
// //         doctorReg1: bill.doctorReg1,
// //         doctorReg2: bill.doctorReg2,
// //         address: bill.address,
// //         age: bill.age,
// //         patientName: bill.patientName || "",
// //         items,
// //       },
// //     });
// //   } catch (err) {
// //     console.error("GET /api/payments/:id error:", err);
// //     res.status(500).json({ error: "Failed to load payment" });
// //   }
// // });

// // // ---------- PDF: Invoice ----------
// // // app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
// // //   const id = req.params.id;
// // //   if (!id) return res.status(400).json({ error: "Invalid bill id" });

// // //   try {
// // //     const billRef = db.collection("bills").doc(id);
// // //     const billSnap = await billRef.get();
// // //     if (!billSnap.exists) {
// // //       return res.status(404).json({ error: "Bill not found" });
// // //     }

// // //     const browser = await puppeteer.launch({
// // //       headless: "new",
// // //       args: ["--no-sandbox", "--disable-setuid-sandbox"],
// // //     });
// // //     const page = await browser.newPage();

// // //     const url = `${FRONTEND_BASE}/print/invoice/${id}`;
// // //     console.log("Generating invoice PDF from URL:", url);

// // //     await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
// // //     await page.waitForSelector("[data-print-ready='1']", { timeout: 60000 });

// // //     const pdfBuffer = await page.pdf({
// // //       format: "A4",
// // //       printBackground: true,
// // //       preferCSSPageSize: false,
// // //       margin: {
// // //         top: "5mm",
// // //         bottom: "5mm",
// // //         left: "5mm",
// // //         right: "5mm",
// // //       },
// // //       pageRanges: "1",
// // //     });

// // //     await browser.close();

// // //     res.setHeader("Content-Type", "application/pdf");
// // //     res.setHeader(
// // //       "Content-Disposition",
// // //       `inline; filename="invoice-${id}.pdf"`
// // //     );
// // //     res.end(pdfBuffer);
// // //   } catch (err) {
// // //     console.error("invoice-html-pdf error:", err);
// // //     if (!res.headersSent) {
// // //       res.status(500).json({ error: "Failed to generate invoice PDF" });
// // //     }
// // //   }
// // // });

// // // ---------- PDF: Invoice (PDFKit) ----------
// // app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
// //   const id = req.params.id;
// //   if (!id) return res.status(400).json({ error: "Invalid bill id" });

// //   try {
// //     // Load bill
// //     const billRef = db.collection("bills").doc(id);
// //     const billSnap = await billRef.get();
// //     if (!billSnap.exists) {
// //       return res.status(404).json({ error: "Bill not found" });
// //     }
// //     const bill = billSnap.data();

// //     // Load items
// //     const itemsSnap = await db
// //       .collection("items")
// //       .where("billId", "==", id)
// //       .get();

// //     const items = itemsSnap.docs.map((doc) => doc.data());

// //     // Prepare response headers
// //     res.setHeader("Content-Type", "application/pdf");
// //     res.setHeader(
// //       "Content-Disposition",
// //       `inline; filename="invoice-${id}.pdf"`
// //     );

// //     // Create PDF
// //     const doc = new PDFDocument({
// //       size: "A4",
// //       margin: 36, // 0.5 inch
// //     });

// //     doc.pipe(res);

// //     // ---------- HEADER ----------
// //     doc
// //       .fontSize(18)
// //       .text("MADHU REKHA EYE CARE", { align: "center" })
// //       .moveDown(0.3);

// //     doc
// //       .fontSize(12)
// //       .text("Invoice", { align: "center" })
// //       .moveDown(1);

// //     // ---------- BILL INFO ----------
// //     doc
// //       .fontSize(10)
// //       .text(`Invoice No: ${bill.invoiceNo || id}`)
// //       .text(`Date: ${bill.date || ""}`)
// //       .moveDown(0.5);

// //     // Patient details
// //     doc
// //       .fontSize(10)
// //       .text(`Patient Name: ${bill.patientName || ""}`)
// //       .text(`Age: ${bill.age ?? ""}`)
// //       .text(`Address: ${bill.address || ""}`)
// //       .moveDown(0.5);

// //     if (bill.doctorReg1) doc.text(`Doctor Reg No 1: ${bill.doctorReg1}`);
// //     if (bill.doctorReg2) doc.text(`Doctor Reg No 2: ${bill.doctorReg2}`);
// //     doc.moveDown(1);

// //     // ---------- ITEMS TABLE ----------
// //     const tableTop = doc.y;

// //     const colDescX = 36;
// //     const colQtyX = 280;
// //     const colRateX = 330;
// //     const colAmountX = 400;

// //     doc.fontSize(10).text("Description", colDescX, tableTop);
// //     doc.text("Qty", colQtyX, tableTop);
// //     doc.text("Rate", colRateX, tableTop);
// //     doc.text("Amount", colAmountX, tableTop);

// //     doc
// //       .moveTo(36, tableTop + 12)
// //       .lineTo(559, tableTop + 12)
// //       .stroke();

// //     let y = tableTop + 18;

// //     items.forEach((item) => {
// //       const qty = Number(item.qty || 0);
// //       const rate = Number(item.rate || 0);
// //       const amount = Number(item.amount || qty * rate);

// //       doc.text(item.description || "", colDescX, y, { width: 230 });
// //       doc.text(qty.toString(), colQtyX, y);
// //       doc.text(rate.toFixed(2), colRateX, y);
// //       doc.text(amount.toFixed(2), colAmountX, y);

// //       y += 16;

// //       // simple page break handling
// //       if (y > 750) {
// //         doc.addPage();
// //         y = 36;
// //       }
// //     });

// //     doc.moveDown(1.5);

// //     // ---------- TOTALS ----------
// //     const subtotal = Number(bill.subtotal || 0);
// //     const adjust = Number(bill.adjust || 0);
// //     const total = Number(bill.total || 0);
// //     const paid = Number(bill.paid || 0);
// //     const balance = Number(bill.balance || 0);

// //     const totalsX = 330;

// //     doc
// //       .fontSize(10)
// //       .text(`Subtotal:`, totalsX, doc.y)
// //       .text(subtotal.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

// //     doc
// //       .text(`Adjustment:`, totalsX, doc.y)
// //       .text(adjust.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

// //     doc
// //       .font("Helvetica-Bold")
// //       .text(`Total:`, totalsX, doc.y)
// //       .text(total.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

// //     doc
// //       .font("Helvetica")
// //       .text(`Paid:`, totalsX, doc.y)
// //       .text(paid.toFixed(2), totalsX + 100, doc.y - 12, { align: "right" });

// //     doc
// //       .font("Helvetica-Bold")
// //       .text(`Balance:`, totalsX, doc.y)
// //       .text(balance.toFixed(2), totalsX + 100, doc.y - 12, {
// //         align: "right",
// //       });

// //     doc.moveDown(2);

// //     // ---------- FOOTER ----------
// //     doc
// //       .fontSize(9)
// //       .font("Helvetica-Oblique")
// //       .text("This is a computer-generated invoice.", {
// //         align: "center",
// //       });

// //     doc.end();
// //   } catch (err) {
// //     console.error("invoice-html-pdf error:", err);
// //     if (!res.headersSent) {
// //       res.status(500).json({ error: "Failed to generate invoice PDF" });
// //     }
// //   }
// // });


// // // ---------- PDF: Receipt ----------
// // // app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
// // //   const id = req.params.id;
// // //   if (!id) return res.status(400).json({ error: "Invalid payment id" });

// // //   try {
// // //     const paymentRef = db.collection("payments").doc(id);
// // //     const paymentSnap = await paymentRef.get();
// // //     if (!paymentSnap.exists) {
// // //       return res.status(404).json({ error: "Payment not found" });
// // //     }

// // //     const browser = await puppeteer.launch({
// // //       headless: "new",
// // //       args: ["--no-sandbox", "--disable-setuid-sandbox"],
// // //     });
// // //     const page = await browser.newPage();

// // //     const url = `${FRONTEND_BASE}/print/receipt/${id}`;
// // //     console.log("Generating receipt PDF from URL:", url);

// // //     await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
// // //     await page.waitForSelector("[data-print-ready='1']", { timeout: 60000 });

// // //     const pdfBuffer = await page.pdf({
// // //       width: "210mm",
// // //       height: "148mm",
// // //       printBackground: true,
// // //       preferCSSPageSize: false,
// // //       margin: {
// // //         top: "3mm",
// // //         bottom: "3mm",
// // //         left: "3mm",
// // //         right: "3mm",
// // //       },
// // //       pageRanges: "1",
// // //       scale: 0.95,
// // //     });

// // //     await browser.close();

// // //     res.setHeader("Content-Type", "application/pdf");
// // //     res.setHeader(
// // //       "Content-Disposition",
// // //       `inline; filename="receipt-${id}.pdf"`
// // //     );
// // //     res.end(pdfBuffer);
// // //   } catch (err) {
// // //     console.error("receipt-html-pdf error:", err);
// // //     if (!res.headersSent) {
// // //       res.status(500).json({ error: "Failed to generate receipt PDF" });
// // //     }
// // //   }
// // // });

// // // ---------- PDF: Receipt (PDFKit) ----------
// // app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
// //   const id = req.params.id;
// //   if (!id) return res.status(400).json({ error: "Invalid payment id" });

// //   try {
// //     // 1) Load this payment
// //     const paymentRef = db.collection("payments").doc(id);
// //     const paymentSnap = await paymentRef.get();
// //     if (!paymentSnap.exists) {
// //       return res.status(404).json({ error: "Payment not found" });
// //     }
// //     const payment = paymentSnap.data();
// //     const billId = payment.billId;

// //     // 2) Load bill
// //     const billRef = db.collection("bills").doc(billId);
// //     const billSnap = await billRef.get();
// //     if (!billSnap.exists) {
// //       return res.status(404).json({ error: "Bill not found" });
// //     }
// //     const bill = billSnap.data();

// //     // 3) Load all payments for this bill to compute cumulative paid & balance
// //     const paysSnap = await db
// //       .collection("payments")
// //       .where("billId", "==", billId)
// //       .get();

// //     const billTotal = Number(bill.total || 0);

// //     const allPayments = paysSnap.docs
// //       .map((doc) => {
// //         const d = doc.data();
// //         const paymentDateTime =
// //           d.paymentDateTime ||
// //           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
// //         return {
// //           id: doc.id,
// //           paymentDateTime,
// //           amount: Number(d.amount || 0),
// //         };
// //       })
// //       .sort((a, b) => {
// //         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
// //         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
// //         return da - dbb;
// //       });

// //     let cumulativePaid = 0;
// //     let paidTillThis = 0;
// //     let balanceAfterThis = billTotal;

// //     for (const p of allPayments) {
// //       cumulativePaid += p.amount;
// //       if (p.id === id) {
// //         paidTillThis = cumulativePaid;
// //         balanceAfterThis = billTotal - paidTillThis;
// //         break;
// //       }
// //     }

// //     const receiptNo =
// //       payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

// //     // ---------- PDF OUTPUT ----------
// //     res.setHeader("Content-Type", "application/pdf");
// //     res.setHeader(
// //       "Content-Disposition",
// //       `inline; filename="receipt-${id}.pdf"`
// //     );

// //     const doc = new PDFDocument({
// //       size: [595.28, 420], // approx A5 landscape in points
// //       margin: 28,
// //     });

// //     doc.pipe(res);

// //     // HEADER
// //     doc
// //       .fontSize(16)
// //       .text("MADHU REKHA EYE CARE", { align: "center" })
// //       .moveDown(0.3);

// //     doc.fontSize(12).text("Payment Receipt", { align: "center" }).moveDown(1);

// //     // RECEIPT INFO
// //     doc
// //       .fontSize(10)
// //       .text(`Receipt No: ${receiptNo}`)
// //       .text(`Receipt Date: ${payment.paymentDate || ""}`)
// //       .moveDown(0.5);

// //     // PATIENT / BILL INFO
// //     doc
// //       .fontSize(10)
// //       .text(`Bill No: ${bill.invoiceNo || billId}`)
// //       .text(`Bill Date: ${bill.date || ""}`)
// //       .moveDown(0.5);

// //     doc
// //       .text(`Patient Name: ${bill.patientName || ""}`)
// //       .text(`Age: ${bill.age ?? ""}`)
// //       .text(`Address: ${bill.address || ""}`)
// //       .moveDown(0.5);

// //     if (bill.doctorReg1) doc.text(`Doctor Reg No 1: ${bill.doctorReg1}`);
// //     if (bill.doctorReg2) doc.text(`Doctor Reg No 2: ${bill.doctorReg2}`);
// //     doc.moveDown(1);

// //     // PAYMENT DETAILS
// //     doc
// //       .fontSize(10)
// //       .text(`Amount Received: Rs. ${Number(payment.amount || 0).toFixed(2)}`)
// //       .text(`Mode: ${payment.mode || "Cash"}`);

// //     if (payment.referenceNo) {
// //       doc.text(`Reference No: ${payment.referenceNo}`);
// //     }

// //     if (payment.drawnOn) {
// //       doc.text(`Drawn On: ${payment.drawnOn}`);
// //     }

// //     if (payment.drawnAs) {
// //       doc.text(`Drawn As: ${payment.drawnAs}`);
// //     }

// //     doc.moveDown(1);

// //     // BILL SUMMARY AFTER THIS PAYMENT
// //     doc
// //       .fontSize(10)
// //       .text(`Bill Total: Rs. ${billTotal.toFixed(2)}`)
// //       .text(`Total Paid (till this receipt): Rs. ${paidTillThis.toFixed(2)}`)
// //       .font("Helvetica-Bold")
// //       .text(
// //         `Balance After This Payment: Rs. ${balanceAfterThis.toFixed(2)}`
// //       )
// //       .font("Helvetica")
// //       .moveDown(2);

// //     // FOOTER
// //     doc
// //       .fontSize(9)
// //       .font("Helvetica-Oblique")
// //       .text("This is a computer-generated receipt.", {
// //         align: "center",
// //       });

// //     doc.end();
// //   } catch (err) {
// //     console.error("receipt-html-pdf error:", err);
// //     if (!res.headersSent) {
// //       res.status(500).json({ error: "Failed to generate receipt PDF" });
// //     }
// //   }
// // });


// // // ---------- START SERVER ----------
// // app.listen(PORT, () => {
// //   console.log(`Backend running on http://localhost:${PORT}`);
// // });






































// // server.js
// import express from "express";
// import cors from "cors";
// import "dotenv/config";
// import path from "path";
// import { fileURLToPath } from "url";
// import { chromium } from "playwright"; // ✅ Playwright instead of Puppeteer/PDFKit
// import { db } from "./firebaseClient.js";

// const app = express();
// const PORT = process.env.PORT || 4000;

// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);

// // ---------- STATIC FILES (fonts, etc.) ----------
// app.use("/resources", express.static(path.join(__dirname, "resources")));

// // ---------- BASIC MIDDLEWARE ----------
// app.use(cors());
// app.use(express.json());

// // ---------- HELPERS ----------
// function computeStatus(total, paid) {
//   if (!total || total <= 0) return "PENDING";
//   if (paid >= total) return "PAID";
//   if (paid > 0 && paid < total) return "PARTIAL";
//   return "PENDING";
// }

// async function generateReceiptNumber() {
//   const now = Date.now();
//   const random = Math.floor(Math.random() * 1000);
//   const suffix = `${String(now).slice(-6)}${String(random).padStart(3, "0")}`;
//   return `RCP-${suffix}`;
// }

// function formatDateYYYYMMDD(dateStrOrDate) {
//   const d = dateStrOrDate ? new Date(dateStrOrDate) : new Date();
//   const yyyy = d.getFullYear();
//   const mm = String(d.getMonth() + 1).padStart(2, "0");
//   const dd = String(d.getDate()).padStart(2, "0");
//   return `${yyyy}${mm}${dd}`;
// }

// // Turn "Rohit Sharma" => "ROHITSHAR"
// function makeNameSlug(name) {
//   if (!name) return "NONAME";
//   const slug = name
//     .toUpperCase()
//     .replace(/[^A-Z]/g, "")
//     .slice(0, 8);
//   return slug || "NONAME";
// }

// // Bill id + invoiceNo: INV-YYYYMMDD-HHMMSS-NAME
// function generateBillId(patientName, billDateInput) {
//   const datePart = formatDateYYYYMMDD(billDateInput);
//   const now = new Date();
//   const timePart = `${String(now.getHours()).padStart(2, "0")}${String(
//     now.getMinutes()
//   ).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
//   const nameSlug = makeNameSlug(patientName);
//   return `INV-${datePart}-${timePart}-${nameSlug}`;
// }

// // FRONTEND base URL (React app)
// const FRONTEND_BASE =
//   process.env.FRONTEND_BASE_URL ||
//   (process.env.NODE_ENV === "production"
//     ? "https://madhu-rekha-billing-software.vercel.app"
//     : "http://localhost:5173");

// // ---------- HEALTH CHECK ----------
// app.get("/", (_req, res) => {
//   res.send("Backend OK");
// });

// //
// // FIRESTORE SCHEMA:
// //
// // bills:
// //   { patientName, address, age, date, invoiceNo, subtotal, adjust,
// //     total, paid, balance, doctorReg1, doctorReg2, status, createdAt }
// //
// // items:
// //   { billId, description, qty, rate, amount }
// //
// // payments:
// //   { billId, amount, mode, referenceNo, drawnOn, drawnAs,
// //     paymentDate, paymentTime, paymentDateTime, receiptNo }
// //

// // ---------- GET /api/bills (list) ----------
// app.get("/api/bills", async (_req, res) => {
//   try {
//     // Newest first by invoiceNo (INV-YYYYMMDD-HHMMSS-NAME)
//     const snapshot = await db
//       .collection("bills")
//       .orderBy("invoiceNo", "desc")
//       .get();

//     const mapped = snapshot.docs.map((doc) => {
//       const b = doc.data();
//       return {
//         id: doc.id, // e.g. INV-20251206-130757-ROHIT
//         invoiceNo: b.invoiceNo || doc.id,
//         patientName: b.patientName || "",
//         date: b.date || null,
//         total: b.total || 0,
//         paid: b.paid || 0,
//         balance: b.balance || 0,
//         status: b.status || "PENDING",
//       };
//     });

//     res.json(mapped);
//   } catch (err) {
//     console.error("GET /api/bills error:", err);
//     res.status(500).json({ error: "Failed to fetch bills" });
//   }
// });

// // ---------- POST /api/bills (create bill + optional first payment) ----------
// app.post("/api/bills", async (req, res) => {
//   try {
//     const {
//       patientName,
//       address,
//       age,
//       date,
//       doctorReg1,
//       doctorReg2,
//       adjust,
//       pay,
//       paymentMode,
//       referenceNo,
//       drawnOn,
//       drawnAs,
//       services,
//     } = req.body;

//     const jsDate = date || new Date().toISOString().slice(0, 10);

//     const itemsData = (services || []).map((s) => {
//       const qty = Number(s.qty) || 0;
//       const rate = Number(s.rate) || 0;
//       const amount = qty * rate;
//       return {
//         description: s.description || "",
//         qty,
//         rate,
//         amount,
//       };
//     });

//     const subtotal = itemsData.reduce((sum, it) => sum + Number(it.amount), 0);
//     const adj = Number(adjust) || 0;
//     const total = subtotal + adj;
//     const firstPay = Number(pay) || 0;
//     const balance = total - firstPay;

//     const status = computeStatus(total, firstPay);

//     // single ID used everywhere
//     const billId = generateBillId(patientName, jsDate);
//     const createdAt = new Date().toISOString();

//     const billRef = db.collection("bills").doc(billId);
//     const batch = db.batch();

//     // 1) Bill
//     batch.set(billRef, {
//       patientName: patientName || "",
//       address: address || "",
//       age: age ? Number(age) : null,
//       date: jsDate,
//       invoiceNo: billId,
//       doctorReg1: doctorReg1 || null,
//       doctorReg2: doctorReg2 || null,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: firstPay,
//       balance,
//       status,
//       createdAt,
//     });

//     // 2) Items
//     itemsData.forEach((item) => {
//       const itemRef = db.collection("items").doc();
//       batch.set(itemRef, {
//         billId,
//         ...item,
//       });
//     });

//     // 3) Optional first payment
//     let paymentDoc = null;
//     let receiptDoc = null;

//     if (firstPay > 0) {
//       const receiptNo = await generateReceiptNumber();
//       const paymentRef = db.collection("payments").doc();
//       const now = new Date();
//       const paymentDate = jsDate;
//       const paymentTime = now.toTimeString().slice(0, 5);
//       const paymentDateTime = now.toISOString();

//       paymentDoc = {
//         billId,
//         amount: firstPay,
//         mode: paymentMode || "Cash",
//         referenceNo: referenceNo || null,
//         drawnOn: drawnOn || null,
//         drawnAs: drawnAs || null,
//         paymentDate,
//         paymentTime,
//         paymentDateTime,
//         receiptNo,
//       };
//       batch.set(paymentRef, paymentDoc);
//       receiptDoc = { receiptNo };
//     }

//     await batch.commit();

//     res.json({
//       bill: {
//         id: billId,
//         invoiceNo: billId,
//         patientName: patientName || "",
//         address: address || "",
//         age: age ? Number(age) : null,
//         date: jsDate,
//         doctorReg1: doctorReg1 || null,
//         doctorReg2: doctorReg2 || null,
//         subtotal,
//         adjust: adj,
//         total,
//         paid: firstPay,
//         balance,
//         status,
//         items: itemsData,
//         paymentMode: paymentDoc?.mode || null,
//         referenceNo: paymentDoc?.referenceNo || null,
//         drawnOn: paymentDoc?.drawnOn || null,
//         drawnAs: paymentDoc?.drawnAs || null,
//       },
//       payment: paymentDoc,
//       receipt: receiptDoc,
//     });
//   } catch (err) {
//     console.error("POST /api/bills error:", err);
//     res.status(500).json({ error: "Failed to create bill" });
//   }
// });

// // ---------- GET /api/bills/:id (detail + items + payments) ----------
// app.get("/api/bills/:id", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const bill = billSnap.data();

//     // Items
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const items = itemsSnap.docs.map((doc) => ({
//       id: doc.id,
//       ...doc.data(),
//     }));

//     // Payments
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", id)
//       .get();

//     let payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       const paymentDateTime =
//         d.paymentDateTime ||
//         (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);

//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         receiptNo: d.receiptNo || null,
//         date: d.paymentDate || null,
//         time: d.paymentTime || null,
//         paymentDateTime,
//         drawnOn: d.drawnOn || null,
//         drawnAs: d.drawnAs || null,
//       };
//     });

//     // Oldest first in history
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const primaryPayment = payments[0] || null;

//     res.json({
//       id,
//       invoiceNo: bill.invoiceNo || id,
//       patientName: bill.patientName || "",
//       address: bill.address || "",
//       age: bill.age || null,
//       date: bill.date || null,
//       subtotal: bill.subtotal || 0,
//       adjust: bill.adjust || 0,
//       total: bill.total || 0,
//       paid: bill.paid || 0,
//       balance: bill.balance || 0,
//       status: bill.status || "PENDING",
//       doctorReg1: bill.doctorReg1 || null,
//       doctorReg2: bill.doctorReg2 || null,
//       items,
//       payments,
//       paymentMode: primaryPayment?.mode || null,
//       referenceNo: primaryPayment?.referenceNo || null,
//       drawnOn: primaryPayment?.drawnOn || null,
//       drawnAs: primaryPayment?.drawnAs || null,
//     });
//   } catch (err) {
//     console.error("bill detail error:", err);
//     res.status(500).json({ error: "Failed to load bill" });
//   }
// });

// // ---------- POST /api/bills/:id/payments (add partial payment) ----------
// app.post("/api/bills/:id/payments", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) {
//     return res.status(400).json({ error: "Invalid bill id" });
//   }

//   const { amount, mode, referenceNo, drawnOn, drawnAs } = req.body;
//   const numericAmount = Number(amount);

//   if (!numericAmount || numericAmount <= 0) {
//     return res.status(400).json({ error: "Amount must be > 0" });
//   }

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();

//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const bill = billSnap.data();

//     const now = new Date();
//     const paymentDate = now.toISOString().slice(0, 10);
//     const paymentTime = now.toTimeString().slice(0, 5);
//     const paymentDateTime = now.toISOString();
//     const receiptNo = await generateReceiptNumber();

//     const paymentRef = db.collection("payments").doc();
//     const paymentDoc = {
//       billId,
//       amount: numericAmount,
//       mode: mode || "Cash",
//       referenceNo: referenceNo || null,
//       drawnOn: drawnOn || null,
//       drawnAs: drawnAs || null,
//       paymentDate,
//       paymentTime,
//       paymentDateTime,
//       receiptNo,
//     };

//     const newPaid = (bill.paid || 0) + numericAmount;
//     const newBalance = (bill.total || 0) - newPaid;
//     const newStatus = computeStatus(bill.total || 0, newPaid);

//     const batch = db.batch();
//     batch.set(paymentRef, paymentDoc);
//     batch.update(billRef, {
//       paid: newPaid,
//       balance: newBalance,
//       status: newStatus,
//     });

//     await batch.commit();

//     res.status(201).json({
//       id: paymentRef.id,
//       ...paymentDoc,
//     });
//   } catch (err) {
//     console.error("payment error:", err);
//     res.status(500).json({ error: "Payment failed" });
//   }
// });

// // ---------- GET /api/payments/:id (JSON for receipt page) ----------
// app.get("/api/payments/:id", async (req, res) => {
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
//     const billTotal = Number(bill.total || 0);

//     // 3) Load all payments for this bill
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const allPayments = paysSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         const paymentDateTime =
//           d.paymentDateTime ||
//           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//         return { id: doc.id, paymentDateTime, amount: d.amount };
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
//       cumulativePaid += Number(p.amount || 0);
//       if (p.id === id) {
//         paidTillThis = cumulativePaid;
//         balanceAfterThis = billTotal - paidTillThis;
//         break;
//       }
//     }

//     // 4) Load bill items
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", billId)
//       .get();

//     const items = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return {
//         id: doc.id,
//         description: d.description,
//         qty: Number(d.qty),
//         rate: Number(d.rate),
//         amount: Number(d.amount),
//       };
//     });

//     // 5) Response for ReceiptPrintPage
//     res.json({
//       id,
//       amount: Number(payment.amount),
//       mode: payment.mode,
//       referenceNo: payment.referenceNo,
//       drawnOn: payment.drawnOn,
//       drawnAs: payment.drawnAs,
//       paymentDate: payment.paymentDate,
//       receiptNo: payment.receiptNo || `R-${String(id).padStart(4, "0")}`,
//       bill: {
//         id: billId,
//         date: bill.date,
//         subtotal: Number(bill.subtotal),
//         adjust: Number(bill.adjust),
//         total: billTotal,
//         paid: paidTillThis, // up to THIS receipt
//         balance: balanceAfterThis, // after THIS receipt
//         doctorReg1: bill.doctorReg1,
//         doctorReg2: bill.doctorReg2,
//         address: bill.address,
//         age: bill.age,
//         patientName: bill.patientName || "",
//         items,
//       },
//     });
//   } catch (err) {
//     console.error("GET /api/payments/:id error:", err);
//     res.status(500).json({ error: "Failed to load payment" });
//   }
// });

// //
// // ---------- HTML → PDF with PLAYWRIGHT ----------
// // Uses your FRONTEND layout at /print/invoice/:id and /print/receipt/:paymentId
// //

// // Invoice PDF from React page
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   let browser;
//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const url = `${FRONTEND_BASE}/print/invoice/${id}?pdf=1`;
//     console.log("Generating invoice PDF from URL:", url);

//     browser = await chromium.launch({
//       args: ["--no-sandbox", "--disable-setuid-sandbox"],
//     });
//     const page = await browser.newPage();
//     await page.goto(url, { waitUntil: "networkidle", timeout: 60000 });

//     const pdfBuffer = await page.pdf({
//       format: "A4",
//       printBackground: true,
//       margin: {
//         top: "5mm",
//         bottom: "5mm",
//         left: "5mm",
//         right: "5mm",
//       },
//       preferCSSPageSize: false,
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
//     if (browser) {
//       try {
//         await browser.close();
//       } catch {}
//     }
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate invoice PDF" });
//     }
//   }
// });

// // Receipt PDF from React page (A5-ish landscape: 210mm x 80mm)
// app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   let browser;
//   try {
//     const paymentRef = db.collection("payments").doc(id);
//     const paymentSnap = await paymentRef.get();
//     if (!paymentSnap.exists) {
//       return res.status(404).json({ error: "Payment not found" });
//     }

//     const url = `${FRONTEND_BASE}/print/receipt/${id}?pdf=1`;
//     console.log("Generating receipt PDF from URL:", url);

//     browser = await chromium.launch({
//       args: ["--no-sandbox", "--disable-setuid-sandbox"],
//     });
//     const page = await browser.newPage();
//     await page.goto(url, { waitUntil: "networkidle", timeout: 60000 });

//     const pdfBuffer = await page.pdf({
//       width: "210mm",
//       height: "80mm",
//       printBackground: true,
//       margin: {
//         top: "3mm",
//         bottom: "3mm",
//         left: "3mm",
//         right: "3mm",
//       },
//       preferCSSPageSize: false,
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
//     if (browser) {
//       try {
//         await browser.close();
//       } catch {}
//     }
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate receipt PDF" });
//     }
//   }
// });

// // ---------- START SERVER ----------
// app.listen(PORT, () => {
//   console.log(`Backend running on http://localhost:${PORT}`);
// });


































// server.js
import express from "express";
import cors from "cors";
import "dotenv/config";
import path from "path";
import { fileURLToPath } from "url";
import { chromium } from "playwright";
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
        paid: paidTillThis, // up to THIS receipt
        balance: balanceAfterThis, // after THIS receipt
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

// ---------- PLAYWRIGHT BROWSER REUSE ----------
let browserPromise = null;

async function getBrowser() {
  if (!browserPromise) {
    browserPromise = chromium.launch({
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
  }
  return browserPromise;
}

// ---------- PDF: Invoice (HTML → PDF via Playwright, single A4 page) ----------
app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  try {
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists) {
      return res.status(404).json({ error: "Bill not found" });
    }

    const url = `${FRONTEND_BASE}/print/invoice/${id}?pdf=1`;
    console.log("Generating invoice PDF from URL:", url);

    const browser = await getBrowser();
    const page = await browser.newPage();

    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    await page.waitForSelector("[data-print-ready='1']", {
      timeout: 60000,
    });

    const pdfBuffer = await page.pdf({
      format: "A4",
      printBackground: true,
      margin: {
        top: "5mm",
        bottom: "5mm",
        left: "5mm",
        right: "5mm",
      },
      preferCSSPageSize: false,
      pageRanges: "1", // force single page
    });

    await page.close();

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="invoice-${id}.pdf"`
    );
    res.end(pdfBuffer);
  } catch (err) {
    console.error("invoice-html-pdf error:", err);
    if (!res.headersSent) {
      res
        .status(500)
        .json({ error: err.message || "Failed to generate invoice PDF" });
    }
  }
});

// ---------- PDF: Receipt (HTML → PDF via Playwright, A4 with half-page layout) ----------
app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid payment id" });

  try {
    const paymentRef = db.collection("payments").doc(id);
    const paymentSnap = await paymentRef.get();
    if (!paymentSnap.exists) {
      return res.status(404).json({ error: "Payment not found" });
    }

    const url = `${FRONTEND_BASE}/print/receipt/${id}?pdf=1`;
    console.log("Generating receipt PDF from URL:", url);

    const browser = await getBrowser();
    const page = await browser.newPage();

    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    await page.waitForSelector("[data-print-ready='1']", {
      timeout: 60000,
    });

    const pdfBuffer = await page.pdf({
      format: "A4", // full A4 sheet; React layout uses ~half height
      printBackground: true,
      margin: {
        top: "5mm",
        bottom: "5mm",
        left: "5mm",
        right: "5mm",
      },
      preferCSSPageSize: false,
      pageRanges: "1",
    });

    await page.close();

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename="receipt-${id}.pdf"`
    );
    res.end(pdfBuffer);
  } catch (err) {
    console.error("receipt-html-pdf error:", err);
    if (!res.headersSent) {
      res
        .status(500)
        .json({ error: err.message || "Failed to generate receipt PDF" });
    }
  }
});

// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
