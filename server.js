// // server.js
// import express from "express";
// import cors from "cors";
// import "dotenv/config";
// import PDFDocument from "pdfkit";
// import fs from "fs";
// import path from "path";
// import { fileURLToPath } from "url";
// import { db } from "./firebaseClient.js";
// // at top of server.js
// import {
//   syncBillToSheet,
//   syncItemsToSheet,
//   syncPaymentToSheet,
//   syncRefundToSheet,
//   syncDeleteBillFromSheet,
//   syncDeleteItemsFromSheet,
//   syncDeletePaymentsFromSheet,
//   syncDeleteRefundsFromSheet,
//   syncProfileToSheet,
// } from "./sheetIntregation.js";

// // NEW: performance middlewares
// import compression from "compression";
// import NodeCache from "node-cache";

// const app = express();
// const PORT = process.env.PORT || 4000;

// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);

// // ---------- CACHE SETUP ----------
// const cache = new NodeCache({
//   stdTTL: 60, // default 60s
//   checkperiod: 120,
// });

// // tiny helper
// function makeCacheKey(...parts) {
//   return parts.join("::");
// }

// async function getOrSetCache(key, ttlSeconds, fetchFn) {
//   const cached = cache.get(key);
//   if (cached !== undefined) return cached;
//   const fresh = await fetchFn();
//   cache.set(key, fresh, ttlSeconds);
//   return fresh;
// }

// // ---------- STATIC FILES (fonts, etc.) ----------
// app.use("/resources", express.static(path.join(__dirname, "resources")));

// // ---------- BASIC MIDDLEWARE ----------
// app.use(cors());
// app.use(express.json());

// // NEW: enable gzip/deflate compression
// app.use(compression());

// // ---------- HELPERS ----------
// function computeStatus(total, paidEffective) {
//   if (!total || total <= 0) return "PENDING";
//   if (paidEffective >= total) return "PAID";
//   if (paidEffective > 0 && paidEffective < total) return "PARTIAL";
//   return "PENDING";
// }

// // --------- ID HELPERS (FINANCIAL YEAR + SEQUENCES, NO COUNTERS COLLECTION) ----------

// // Returns "25-26" for FY 2025-26 based on Indian FY (Apr–Mar)
// function getFinancialYearCode(dateStrOrDate) {
//   const d = dateStrOrDate ? new Date(dateStrOrDate) : new Date();
//   let year = d.getFullYear();
//   const month = d.getMonth() + 1; // 1-12

//   // Indian FY: if month < April, FY starts previous year
//   let fyStart = month >= 4 ? year : year - 1;
//   let fyEnd = fyStart + 1;

//   const fyStartShort = String(fyStart).slice(-2);
//   const fyEndShort = String(fyEnd).slice(-2);
//   return `${fyStartShort}-${fyEndShort}`; // e.g., "25-26"
// }

// // Generate invoice number WITHOUT counters collection
// async function generateInvoiceNumber(billDateInput) {
//   const dateStr = billDateInput || new Date().toISOString().slice(0, 10);
//   const fy = getFinancialYearCode(dateStr);
//   const prefix = `${fy}/INV-`;

//   const snap = await db
//     .collection("bills")
//     .where("invoiceNo", ">=", prefix)
//     .where("invoiceNo", "<=", prefix + "\uf8ff")
//     .orderBy("invoiceNo", "desc")
//     .limit(1)
//     .get();

//   let nextNumber = 1;
//   if (!snap.empty) {
//     const last = snap.docs[0].data().invoiceNo || "";
//     // extract trailing digits (serial) safely
//     const m = last.match(/(\d+)$/);
//     const current = m ? Number(m[1]) : 0;
//     nextNumber = current + 1;
//   }

//   const serial = String(nextNumber).padStart(4, "0");
//   const invoiceNo = `${fy}/INV-${serial}`;

//   return { invoiceNo, fy, serial };
// }

// // Parse "25-26/INV-0001" into { fy: "25-26", invoiceSerial: "0001" }
// function parseInvoiceNumber(invoiceNo) {
//   const [fy, rest] = (invoiceNo || "").split("/");
//   if (!fy || !rest) return { fy: "00-00", invoiceSerial: "0000" };
//   // get trailing digits after last dash
//   const m = rest.match(/(\d+)$/);
//   const invoiceSerial = m ? String(m[1]).padStart(4, "0") : "0000";
//   return { fy, invoiceSerial };
// }

// // Generate receipt id per invoice WITHOUT counters collection
// // Uses how many payments exist for that bill.
// async function generateReceiptId(invoiceNo, billId) {
//   if (!billId) {
//     throw new Error("billId is required for generateReceiptId");
//   }

//   const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);

//   const snap = await db
//     .collection("payments")
//     .where("billId", "==", billId)
//     .get();

//   const seq = snap.size + 1;
//   const recSerial = String(seq).padStart(4, "0");
//   return `${fy}/INV-${invoiceSerial}/REC-${recSerial}`;
// }

// // Generate refund id per invoice WITHOUT counters collection
// // Uses how many refunds exist for that bill.
// async function generateRefundId(invoiceNo, billId) {
//   if (!billId) {
//     throw new Error("billId is required for generateRefundId");
//   }

//   const { fy, invoiceSerial } = parseInvoiceNumber(invoiceNo);

//   const snap = await db
//     .collection("refunds")
//     .where("billId", "==", billId)
//     .get();

//   const seq = snap.size + 1;
//   const refSerial = String(seq).padStart(4, "0");
//   return `${fy}/INV-${invoiceSerial}/REF-${refSerial}`;
// }

// // FRONTEND base URL (React app)
// const FRONTEND_BASE =
//   process.env.FRONTEND_BASE_URL ||
//   (process.env.NODE_ENV === "production"
//     ? "https://madhu-rekha-billing-software.vercel.app"
//     : "http://localhost:5173");

// // ---------- CLINIC PROFILE HELPER (cache by default; allow force fresh read) ----------
// async function getClinicProfile({ force = false } = {}) {
//   const key = makeCacheKey("profile", "clinic");
//   if (force) {
//     const snap = await db.collection("settings").doc("clinicProfile").get();
//     const data = snap.exists ? snap.data() : null;
//     if (data) cache.set(key, data, 300); // update cache for other readers
//     return data;
//   }
//   return await getOrSetCache(key, 300, async () => {
//     const snap = await db.collection("settings").doc("clinicProfile").get();
//     return snap.exists ? snap.data() : null;
//   });
// }

// // safe accessor (returns empty string if missing) — avoids 'undefined' in PDFs
// function profileValue(profile, key) {
//   if (!profile) return "";
//   const v = profile[key];
//   if (typeof v === "undefined" || v === null) return "";
//   return String(v);
// }

// // ---------- HEALTH CHECK ----------
// app.get("/", (_req, res) => {
//   res.send("Backend OK");
// });

// //
// // FIRESTORE SCHEMA (doctorReg removed):
// //
// // bills:
// //   { patientName, sex, address, age, date, invoiceNo, subtotal, adjust,
// //     total, paid, refunded, balance, status, createdAt, remarks, services: [...] }
// // ... (unchanged comments)
// //

// // ---------- GET /api/dashboard/summary ----------
// app.get("/api/dashboard/summary", async (_req, res) => {
//   try {
//     const key = makeCacheKey("dashboard", "summary");
//     const data = await getOrSetCache(key, 60, async () => {
//       const now = new Date();
//       const yyyy = now.getFullYear();
//       const mm = String(now.getMonth() + 1).padStart(2, "0");
//       const dd = String(now.getDate()).padStart(2, "0");

//       const todayStr = `${yyyy}-${mm}-${dd}`;
//       const monthStart = `${yyyy}-${mm}-01`;
//       const monthEnd = `${yyyy}-${mm}-31`;
//       const yearStart = `${yyyy}-01-01`;
//       const yearEnd = `${yyyy}-12-31`;

//       async function sumPaymentsRange(start, end) {
//         const snap = await db
//           .collection("payments")
//           .where("paymentDate", ">=", start)
//           .where("paymentDate", "<=", end)
//           .get();

//         let total = 0;
//         let count = 0;
//         snap.forEach((doc) => {
//           total += Number(doc.data().amount || 0);
//           count++;
//         });
//         return { total, count };
//       }

//       async function sumRefundsRange(start, end) {
//         const snap = await db
//           .collection("refunds")
//           .where("refundDate", ">=", start)
//           .where("refundDate", "<=", end)
//           .get();

//         let total = 0;
//         let count = 0;
//         snap.forEach((doc) => {
//           total += Number(doc.data().amount || 0);
//           count++;
//         });
//         return { total, count };
//       }

//       const todayPaymentsSnap = await db
//         .collection("payments")
//         .where("paymentDate", "==", todayStr)
//         .get();
//       let todayPayTotal = 0;
//       let todayPayCount = 0;
//       todayPaymentsSnap.forEach((doc) => {
//         todayPayTotal += Number(doc.data().amount || 0);
//         todayPayCount++;
//       });

//       const todayRefundsSnap = await db
//         .collection("refunds")
//         .where("refundDate", "==", todayStr)
//         .get();
//       let todayRefundTotal = 0;
//       let todayRefundCount = 0;
//       todayRefundsSnap.forEach((doc) => {
//         todayRefundTotal += Number(doc.data().amount || 0);
//         todayRefundCount++;
//       });

//       const todayNet = todayPayTotal - todayRefundTotal;

//       const monthPayments = await sumPaymentsRange(monthStart, monthEnd);
//       const monthRefunds = await sumRefundsRange(monthStart, monthEnd);
//       const monthNet = monthPayments.total - monthRefunds.total;

//       const yearPayments = await sumPaymentsRange(yearStart, yearEnd);
//       const yearRefunds = await sumRefundsRange(yearStart, yearEnd);
//       const yearNet = yearPayments.total - yearRefunds.total;

//       return {
//         today: {
//           label: todayStr,
//           paymentsTotal: todayPayTotal,
//           paymentsCount: todayPayCount,
//           refundsTotal: todayRefundTotal,
//           refundsCount: todayRefundCount,
//           netTotal: todayNet,
//         },
//         month: {
//           label: `${yyyy}-${mm}`,
//           paymentsTotal: monthPayments.total,
//           paymentsCount: monthPayments.count,
//           refundsTotal: monthRefunds.total,
//           refundsCount: monthRefunds.count,
//           netTotal: monthNet,
//         },
//         year: {
//           label: `${yyyy}`,
//           paymentsTotal: yearPayments.total,
//           paymentsCount: yearPayments.count,
//           refundsTotal: yearRefunds.total,
//           refundsCount: yearRefunds.count,
//           netTotal: yearNet,
//         },
//       };
//     });

//     res.json(data);
//   } catch (err) {
//     console.error("dashboard summary error:", err);
//     res.status(500).json({ error: "Failed to load dashboard summary" });
//   }
// });

// // ---------- GET /api/bills (list) ----------
// app.get("/api/bills", async (_req, res) => {
//   try {
//     const key = makeCacheKey("bills", "list");
//     const mapped = await getOrSetCache(key, 30, async () => {
//       const snapshot = await db
//         .collection("bills")
//         .orderBy("invoiceNo", "desc")
//         .get();

//       return snapshot.docs.map((doc) => {
//         const b = doc.data();
//         const total = Number(b.total || 0);
//         const paidGross = Number(b.paid || 0); // all payments
//         const refunded = Number(b.refunded || 0); // all refunds
//         const paidNet = paidGross - refunded;
//         const balance = b.balance != null ? Number(b.balance) : total - paidNet;

//         return {
//           id: doc.id,
//           invoiceNo: b.invoiceNo || doc.id,
//           patientName: b.patientName || "",
//           date: b.date || null,
//           total,
//           paid: paidNet,
//           refunded,
//           balance,
//           status: b.status || "PENDING",
//         };
//       });
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
//       sex,
//       address,
//       age,
//       date,
//       adjust,
//       pay,
//       paymentMode,
//       referenceNo,

//       // NEW – mode-specific payment fields from CreateBill
//       chequeDate,
//       chequeNumber,
//       bankName,
//       transferType,
//       transferDate,
//       upiName,
//       upiId,
//       upiDate,

//       drawnOn,
//       drawnAs,

//       // generic remarks
//       remarks,

//       // service rows from CreateBill
//       services,
//     } = req.body;

//     const jsDate = date || new Date().toISOString().slice(0, 10);

//     // 1) SERVICES ko normalize karo
//     const normalizedServices = Array.isArray(services)
//       ? services.map((s) => {
//           const qty = Number(s.qty) || 0;
//           const rate = Number(s.rate) || 0;
//           const amount = qty * rate;
//           return {
//             item: s.item || "",
//             details: s.details || "",
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     // 2) ITEMS DATA – items collection + sheet ke liye
//     const itemsData = normalizedServices.map((s) => {
//       const parts = [];
//       if (s.item) parts.push(s.item);
//       if (s.details) parts.push(s.details);
//       const description = parts.join(" - ") || "";
//       return {
//         description,
//         qty: s.qty,
//         rate: s.rate,
//         amount: s.amount,
//       };
//     });

//     // 3) Totals
//     const subtotal = itemsData.reduce(
//       (sum, it) => sum + Number(it.amount || 0),
//       0
//     );
//     const adj = Number(adjust) || 0;
//     const total = subtotal + adj;

//     const firstPay = Number(pay) || 0;
//     const refunded = 0;
//     const effectivePaid = firstPay - refunded;
//     const balance = total - effectivePaid;
//     const status = computeStatus(total, effectivePaid);

//     // 4) Invoice no + billId generate
//     const { invoiceNo } = await generateInvoiceNumber(jsDate);
//     const billId = invoiceNo.replace(/\//g, "_"); // e.g. "25-26_INV-0001"
//     const createdAt = new Date().toISOString();

//     const billRef = db.collection("bills").doc(billId);
//     const batch = db.batch();

//     // 5) Bill document (doctor regnos removed)
//     batch.set(billRef, {
//       patientName: patientName || "",
//       sex: sex || null,
//       address: address || "",
//       age: age ? Number(age) : null,
//       date: jsDate,
//       invoiceNo: invoiceNo,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: firstPay,
//       refunded,
//       balance,
//       status,
//       createdAt,
//       remarks: remarks || null,
//       // store normalized services on the bill for PDFs / future UI
//       services: normalizedServices,
//     });

//     // 6) Items collection (1 doc per item row)
//     itemsData.forEach((item, index) => {
//       const lineNo = index + 1;
//       const itemId = `${billId}-${String(lineNo).padStart(2, "0")}`;

//       const qty = Number(item.qty || 0);
//       const rate = Number(item.rate || 0);
//       const amount = qty * rate;

//       const itemRef = db.collection("items").doc(itemId);

//       batch.set(itemRef, {
//         billId,
//         patientName: patientName || "",
//         description: item.description || "",
//         qty,
//         rate,
//         amount,
//       });
//     });

//     // 7) Optional first payment
//     let paymentDoc = null;
//     let receiptDoc = null;

//     if (firstPay > 0) {
//       const receiptNo = await generateReceiptId(invoiceNo, billId);
//       const paymentId = receiptNo.replace(/\//g, "_");
//       const paymentRef = db.collection("payments").doc(paymentId);
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

//         // persist mode-specific extras for first payment too
//         chequeDate: chequeDate || null,
//         chequeNumber: chequeNumber || null,
//         bankName: bankName || null,

//         transferType: transferType || null,
//         transferDate: transferDate || null,

//         upiName: upiName || null,
//         upiId: upiId || null,
//         upiDate: upiDate || null,

//         paymentDate,
//         paymentTime,
//         paymentDateTime,
//         receiptNo,
//       };

//       batch.set(paymentRef, paymentDoc);
//       receiptDoc = { id: paymentId, receiptNo };
//     }

//     await batch.commit();

//     // cache clear
//     cache.flushAll();

//     // 8) Sheets sync (fire-and-forget)
//     syncBillToSheet({
//       id: billId,
//       invoiceNo: invoiceNo,
//       patientName,
//       address,
//       age: age ? Number(age) : null,
//       date: jsDate,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: firstPay,
//       refunded,
//       balance,
//       status,
//       sex: sex || null,
//     });

//     syncItemsToSheet(
//       billId,
//       billId,
//       patientName,
//       itemsData.map((it) => ({
//         description: it.description,
//         qty: it.qty,
//         rate: it.rate,
//         amount: it.amount,
//       }))
//     );

//     // 9) Response (doctorReg removed)
//     res.json({
//       bill: {
//         id: billId,
//         invoiceNo: invoiceNo,
//         patientName: patientName || "",
//         sex: sex || null,
//         address: address || "",
//         age: age ? Number(age) : null,
//         date: jsDate,
//         subtotal,
//         adjust: adj,
//         total,
//         paid: firstPay,
//         refunded,
//         balance,
//         status,
//         remarks: remarks || null,
//         services: normalizedServices,
//         paymentMode: paymentDoc?.mode || null,
//         referenceNo: paymentDoc?.referenceNo || null,
//         drawnOn: paymentDoc?.drawnOn || null,
//         drawnAs: paymentDoc?.drawnAs || null,
//         chequeDate: paymentDoc?.chequeDate || null,
//         chequeNumber: paymentDoc?.chequeNumber || null,
//         bankName: paymentDoc?.bankName || null,
//         transferType: paymentDoc?.transferType || null,
//         transferDate: paymentDoc?.transferDate || null,
//         upiName: paymentDoc?.upiName || null,
//         upiId: paymentDoc?.upiId || null,
//         upiDate: paymentDoc?.upiDate || null,
//       },
//       payment: paymentDoc,
//       receipt: receiptDoc,
//     });
//   } catch (err) {
//     console.error("POST /api/bills error:", err);
//     res.status(500).json({ error: "Failed to create bill" });
//   }
// });

// // ---------- GET /api/bills/:id (detail + items + payments + refunds) ----------
// app.get("/api/bills/:id", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const key = makeCacheKey("bill-detail", id);
//     const data = await getOrSetCache(key, 30, async () => {
//       const billRef = db.collection("bills").doc(id);
//       const billSnap = await billRef.get();
//       if (!billSnap.exists) {
//         throw new Error("NOT_FOUND");
//       }

//       const bill = billSnap.data();

//       // Items
//       const itemsSnap = await db
//         .collection("items")
//         .where("billId", "==", id)
//         .get();

//       const items = itemsSnap.docs.map((doc) => ({
//         id: doc.id,
//         ...doc.data(),
//       }));

//       // Payments
//       const paysSnap = await db
//         .collection("payments")
//         .where("billId", "==", id)
//         .get();

//       let payments = paysSnap.docs.map((doc) => {
//         const d = doc.data();
//         const paymentDateTime =
//           d.paymentDateTime ||
//           (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);

//         return {
//           id: doc.id,
//           amount: Number(d.amount || 0),
//           mode: d.mode || "",
//           referenceNo: d.referenceNo || null,
//           receiptNo: d.receiptNo || null,
//           date: d.paymentDate || null,
//           time: d.paymentTime || null,
//           paymentDateTime,
//           drawnOn: d.drawnOn || null,
//           drawnAs: d.drawnAs || null,
//         };
//       });

//       payments.sort((a, b) => {
//         const da = a.paymentDateTime
//           ? new Date(a.paymentDateTime)
//           : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//       const totalPaidGross = payments.reduce(
//         (sum, p) => sum + Number(p.amount || 0),
//         0
//       );

//       // Refunds
//       const refundsSnap = await db
//         .collection("refunds")
//         .where("billId", "==", id)
//         .get();

//       let refunds = refundsSnap.docs.map((doc) => {
//         const d = doc.data();
//         const refundDateTime =
//           d.refundDateTime ||
//           (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
//         return {
//           id: doc.id,
//           amount: Number(d.amount || 0),
//           mode: d.mode || "",
//           referenceNo: d.referenceNo || null,
//           refundNo: d.refundReceiptNo || null,
//           date: d.refundDate || null,
//           time: d.refundTime || null,
//           refundDateTime,
//           drawnOn: d.drawnOn || null,
//           drawnAs: d.drawnAs || null,
//         };
//       });

//       refunds.sort((a, b) => {
//         const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
//         const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
//         return da - dbb;
//       });

//       const totalRefunded = refunds.reduce(
//         (sum, r) => sum + Number(r.amount || 0),
//         0
//       );

//       const total = Number(bill.total || 0);
//       const netPaid = totalPaidGross - totalRefunded;
//       const balance = total - netPaid;
//       const status = computeStatus(total, netPaid);

//       const primaryPayment = payments[0] || null;

//       return {
//         id,
//         invoiceNo: bill.invoiceNo || id,
//         patientName: bill.patientName || "",
//         sex: bill.sex || null,
//         address: bill.address || "",
//         age: bill.age || null,
//         date: bill.date || null,
//         subtotal: Number(bill.subtotal || 0),
//         adjust: Number(bill.adjust || 0),
//         total,
//         paid: netPaid,
//         refunded: totalRefunded,
//         totalPaid: totalPaidGross,
//         balance,
//         status,
//         // doctorReg1 and doctorReg2 intentionally omitted (static in PDFs only)
//         remarks: bill.remarks || null,
//         services: bill.services || null,
//         items,
//         payments,
//         refunds,
//         paymentMode: primaryPayment?.mode || null,
//         referenceNo: primaryPayment?.referenceNo || null,
//         drawnOn: primaryPayment?.drawnOn || null,
//         drawnAs: primaryPayment?.drawnAs || null,
//       };
//     });

//     res.json(data);
//   } catch (err) {
//     if (err.message === "NOT_FOUND") {
//       return res.status(404).json({ error: "Bill not found" });
//     }
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

//   const {
//     amount,
//     mode,
//     referenceNo,
//     drawnOn,
//     drawnAs,

//     // NEW – mode-specific fields
//     chequeDate,
//     chequeNumber,
//     bankName,
//     transferType,
//     transferDate,
//     upiName,
//     upiId,
//     upiDate,
//   } = req.body;

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

//     const invoiceNo = bill.invoiceNo || billId;
//     const receiptNo = await generateReceiptId(invoiceNo, billId); // pass billId
//     const paymentId = receiptNo.replace(/\//g, "_");
//     const paymentRef = db.collection("payments").doc(paymentId);
//     const paymentDoc = {
//       billId,
//       amount: numericAmount,
//       mode: mode || "Cash",
//       referenceNo: referenceNo || null,
//       drawnOn: drawnOn || null,
//       drawnAs: drawnAs || null,

//       // NEW – mode-specific fields persisted
//       chequeDate: chequeDate || null,
//       chequeNumber: chequeNumber || null,
//       bankName: bankName || null,

//       transferType: transferType || null,
//       transferDate: transferDate || null,

//       upiName: upiName || null,
//       upiId: upiId || null,
//       upiDate: upiDate || null,

//       paymentDate,
//       paymentTime,
//       paymentDateTime,
//       receiptNo,
//     };

//     const oldPaid = Number(bill.paid || 0);
//     const oldRefunded = Number(bill.refunded || 0);
//     const newPaid = oldPaid + numericAmount;
//     const effectivePaid = newPaid - oldRefunded;
//     const total = Number(bill.total || 0);
//     const newBalance = total - effectivePaid;
//     const newStatus = computeStatus(total, effectivePaid);

//     const batch = db.batch();
//     batch.set(paymentRef, paymentDoc);
//     batch.update(billRef, {
//       paid: newPaid,
//       balance: newBalance,
//       status: newStatus,
//     });

//     await batch.commit();

//     // invalidate cache on write
//     cache.flushAll();

//     // sync to sheet
//     syncPaymentToSheet(
//       { id: paymentRef.id, ...paymentDoc },
//       { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
//     );

//     res.status(201).json({
//       id: paymentId,
//       ...paymentDoc,
//     });
//   } catch (err) {
//     console.error("payment error:", err);
//     res.status(500).json({ error: "Payment failed" });
//   }
// });

// // ---------- POST /api/bills/:id/refunds (issue refund) ----------
// app.post("/api/bills/:id/refunds", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) {
//     return res.status(400).json({ error: "Invalid bill id" });
//   }

//   const {
//     amount,
//     mode,
//     referenceNo,
//     drawnOn,
//     drawnAs,

//     // NEW – mode-specific fields
//     chequeDate,
//     chequeNumber,
//     bankName,
//     transferType,
//     transferDate,
//     upiName,
//     upiId,
//     upiDate,
//   } = req.body;

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
//     const total = Number(bill.total || 0);
//     const paidGross = Number(bill.paid || 0);
//     const alreadyRefunded = Number(bill.refunded || 0);
//     const netPaidBefore = paidGross - alreadyRefunded;

//     if (numericAmount > netPaidBefore) {
//       return res.status(400).json({
//         error: "Cannot refund more than net paid amount",
//       });
//     }

//     const now = new Date();
//     const refundDate = now.toISOString().slice(0, 10);
//     const refundTime = now.toTimeString().slice(0, 5);
//     const refundDateTime = now.toISOString();
//     const invoiceNo = bill.invoiceNo || billId;
//     const refundReceiptNo = await generateRefundId(invoiceNo, billId); // pass billId
//     const refundId = refundReceiptNo.replace(/\//g, "_");
//     const refundRef = db.collection("refunds").doc(refundId);

//     const refundDoc = {
//       billId,
//       amount: numericAmount,
//       mode: mode || "Cash",
//       referenceNo: referenceNo || null,
//       drawnOn: drawnOn || null,
//       drawnAs: drawnAs || null,

//       // NEW – mode-specific fields persisted
//       chequeDate: chequeDate || null,
//       chequeNumber: chequeNumber || null,
//       bankName: bankName || null,

//       transferType: transferType || null,
//       transferDate: transferDate || null,

//       upiName: upiName || null,
//       upiId: upiId || null,
//       upiDate: upiDate || null,

//       refundDate,
//       refundTime,
//       refundDateTime,
//       refundReceiptNo,
//     };

//     const newRefunded = alreadyRefunded + numericAmount;
//     const effectivePaid = paidGross - newRefunded;
//     const newBalance = total - effectivePaid;
//     const newStatus = computeStatus(total, effectivePaid);

//     const batch = db.batch();
//     batch.set(refundRef, refundDoc);
//     batch.update(billRef, {
//       refunded: newRefunded,
//       balance: newBalance,
//       status: newStatus,
//     });

//     await batch.commit();

//     // invalidate cache on write
//     cache.flushAll();

//     syncRefundToSheet(
//       {
//         id: refundRef.id,
//         ...refundDoc,
//         netPaidAfterThis: effectivePaid,
//         balanceAfterThis: newBalance,
//       },
//       { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
//     );

//     res.status(201).json({
//       id: refundId,
//       ...refundDoc,
//     });
//   } catch (err) {
//     console.error("refund error:", err);
//     res.status(500).json({ error: "Refund failed" });
//   }
// });

// // ---------- GET /api/payments/:id (JSON for receipt page) ----------
// app.get("/api/payments/:id", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   try {
//     const key = makeCacheKey("payment-detail", id);
//     const data = await getOrSetCache(key, 30, async () => {
//       const paymentRef = db.collection("payments").doc(id);
//       const paymentSnap = await paymentRef.get();

//       if (!paymentSnap.exists) {
//         throw new Error("NOT_FOUND");
//       }

//       const payment = paymentSnap.data();
//       const billId = payment.billId;

//       const billRef = db.collection("bills").doc(billId);
//       const billSnap = await billRef.get();

//       if (!billSnap.exists) {
//         throw new Error("BILL_NOT_FOUND");
//       }

//       const bill = billSnap.data();
//       const billTotal = Number(bill.total || 0);

//       const paysSnap = await db
//         .collection("payments")
//         .where("billId", "==", billId)
//         .get();

//       const allPayments = paysSnap.docs
//         .map((doc) => {
//           const d = doc.data();
//           const paymentDateTime =
//             d.paymentDateTime ||
//             (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//           return { id: doc.id, paymentDateTime, amount: Number(d.amount || 0) };
//         })
//         .sort((a, b) => {
//           const da = a.paymentDateTime
//             ? new Date(a.paymentDateTime)
//             : new Date(0);
//           const dbb = b.paymentDateTime
//             ? new Date(b.paymentDateTime)
//             : new Date(0);
//           return da - dbb;
//         });

//       let cumulativePaid = 0;
//       let paidTillThis = 0;
//       let balanceAfterThis = billTotal;

//       for (const p of allPayments) {
//         cumulativePaid += Number(p.amount || 0);
//         if (p.id === id) {
//           paidTillThis = cumulativePaid;
//           balanceAfterThis = billTotal - paidTillThis;
//           break;
//         }
//       }

//       const itemsSnap = await db
//         .collection("items")
//         .where("billId", "==", billId)
//         .get();

//       const items = itemsSnap.docs.map((doc) => {
//         const d = doc.data();
//         return {
//           id: doc.id,
//           description: d.description,
//           qty: Number(d.qty),
//           rate: Number(d.rate),
//           amount: Number(d.amount),
//         };
//       });

//       return {
//         id,
//         amount: Number(payment.amount),
//         mode: payment.mode,
//         referenceNo: payment.referenceNo,
//         drawnOn: payment.drawnOn,
//         drawnAs: payment.drawnAs,
//         paymentDate: payment.paymentDate,
//         receiptNo: payment.receiptNo || `R-${String(id).padStart(4, "0")}`,
//         bill: {
//           id: billId,
//           date: bill.date,
//           subtotal: Number(bill.subtotal),
//           adjust: Number(bill.adjust),
//           total: billTotal,
//           paid: paidTillThis,
//           balance: balanceAfterThis,
//           // doctorReg removed intentionally (PDF header is static)
//           address: bill.address,
//           age: bill.age,
//           patientName: bill.patientName || "",
//           items,
//         },
//       };
//     });

//     res.json(data);
//   } catch (err) {
//     if (err.message === "NOT_FOUND") {
//       return res.status(404).json({ error: "Payment not found" });
//     }
//     if (err.message === "BILL_NOT_FOUND") {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     console.error("GET /api/payments/:id error:", err);
//     res.status(500).json({ error: "Failed to load payment" });
//   }
// });

// // ---------- PDF: Invoice (A4 full page) ----------
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//         const da = a.paymentDateTime
//           ? new Date(a.paymentDateTime)
//           : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//     const primaryPayment = payments[0] || null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let subtotal = Number(bill.subtotal || 0);
//     let adjust = Number(bill.adjust || 0);
//     let total = Number(bill.total || 0);

//     if (!subtotal && items.length > 0) {
//       subtotal = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }
//     if (!total && subtotal) {
//       total = subtotal + adjust;
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = bill.date || "";
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colAdjW = 60;
//     const colSubW = 70;
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

//     const colSrX = tableLeft;
//     const colQtyX = colSrX + colSrW;
//     const colServiceX = colQtyX + colQtyW;
//     const colRateX = colServiceX + colServiceW;
//     const colAdjX = colRateX + colRateW;
//     const colSubX = colAdjX + colAdjW;

//     // header background
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, 16)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Procedure", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Rate / Price", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Adjust", colAdjX + 2, y + 3, {
//       width: colAdjW - 4,
//       align: "right",
//     });
//     doc.text("Sub Total", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += 16;
//     doc.font("Helvetica").fontSize(9);

//     items.forEach((item, idx) => {
//       const rowHeight = 14;

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text("0.00", colAdjX + 2, y + 3, {
//         width: colAdjW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       y += rowHeight;

//       if (y > doc.page.height - 200) {
//         doc.addPage();
//         y = 36;
//       }
//     });

//     y += 12;

//     // 9) TOTALS BOX
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     doc.rect(boxX, boxY, boxWidth, lineH * 5 + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     doc.text("Sub Total", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(subtotal)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.text("Adjust", boxX + 6, boxY + 2 + lineH);
//     doc.text(`Rs ${formatMoney(adjust)}`, boxX, boxY + 2 + lineH, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.text("Tax", boxX + 6, boxY + 2 + lineH * 2);
//     doc.text("Rs 0.00", boxX, boxY + 2 + lineH * 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.text("Refunded", boxX + 6, boxY + 2 + lineH * 3);
//     doc.text(`Rs ${formatMoney(totalRefunded)}`, boxX, boxY + 2 + lineH * 3, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2 + lineH * 4);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2 + lineH * 4, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // move below totals box
//     y = boxY + lineH * 5 + 20;

//     // NET PAID + BALANCE
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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

// // ---------- PDF: Payment Receipt (A4 half page, professional layout) ----------
// app.get("/api/payments/:id/receipt-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid payment id" });

//   try {
//     const paymentRef = db.collection("payments").doc(id);
//     const paymentSnap = await paymentRef.get();
//     if (!paymentSnap.exists) {
//       return res.status(404).json({ error: "Payment not found" });
//     }
//     const payment = paymentSnap.data();
//     const billId = payment.billId;

//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();
//     const billTotal = Number(bill.total || 0);

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
//         return {
//           id: doc.id,
//           paymentDateTime,
//           amount: Number(d.amount || 0),
//         };
//       })
//       .sort((a, b) => {
//         const da = a.paymentDateTime
//           ? new Date(a.paymentDateTime)
//           : new Date(0);
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

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const patientName = bill.patientName || "";
//     const drawnOn = payment.drawnOn || null;
//     const drawnAs = payment.drawnAs || null;
//     const mode = payment.mode || "Cash";
//     const referenceNo = payment.referenceNo || null;
//     const receiptNo = payment.receiptNo || `R-${String(id).padStart(4, "0")}`;

//     const chequeDate = payment.chequeDate || null;
//     const chequeNumber = payment.chequeNumber || null;
//     const bankName = payment.bankName || null;
//     const transferType = payment.transferType || null;
//     const transferDate = payment.transferDate || null;
//     const upiName = payment.upiName || null;
//     const upiId = payment.upiId || null;
//     const upiDate = payment.upiDate || null;

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="receipt-${id}.pdf"`
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
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();
//     y += 6;

//     // DOCTOR LINE (from profile)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 16;

//     // TITLE BAR
//     doc
//       .save()
//       .rect(36, y, usableWidth, 18)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 18)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     // COMMON LAYOUT (left details + right summary)
//     const isPayment = true;
//     const leftX = 36;
//     const rightBoxWidth = 180;
//     const rightX = pageWidth - 36 - rightBoxWidth;

//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Receipt No.: ${receiptNo}`, leftX, y);
//     doc.text(`Date: ${payment.paymentDate || ""}`, rightX, y, {
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

//     let detailsY = doc.y + 4;
//     doc.font("Helvetica");

//     const addDetail = (label, value) => {
//       if (!value) return;
//       doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
//       detailsY = doc.y + 3;
//     };

//     addDetail("Amount Received: Rs", formatMoney(payment.amount));
//     addDetail("Payment Mode:", mode);
//     addDetail("Cheque No.:", chequeNumber);
//     addDetail("Cheque Date:", chequeDate);
//     addDetail("Bank:", bankName);
//     addDetail("Transfer Type:", transferType);
//     addDetail("Transfer Date:", transferDate);
//     addDetail("UPI ID:", upiId);
//     addDetail("UPI Name:", upiName);
//     addDetail("UPI Date:", upiDate);
//     addDetail("Reference No.:", referenceNo);
//     addDetail("Drawn On:", drawnOn);
//     addDetail("Drawn As:", drawnAs);

//     // RIGHT BILL SUMMARY BOX
//     const boxY = detailsTopY;
//     const lineH = 12;
//     const rows = 5; // Bill No, Date, Total, Paid, Balance
//     const boxHeight = 100;

//     doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

//     let by = boxY + 4;
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(9)
//       .text("Bill Summary", rightX + 6, by);
//     by += lineH + 2;
//     doc.font("Helvetica").fontSize(9);

//     const billNoText = bill.invoiceNo || billId;

//     const addRow = (label, value) => {
//       doc.text(label, rightX + 6, by);
//       doc.text(value, rightX + 6, by, {
//         width: rightBoxWidth - 12,
//         align: "right",
//       });
//       by += lineH;
//     };

//     addRow("Bill No.:", billNoText);
//     addRow("Bill Date:", bill.date || "");
//     addRow("Bill Total:", `Rs ${formatMoney(billTotal)}`);
//     addRow("Paid (incl. this):", `Rs ${formatMoney(paidTillThis)}`);
//     addRow("Balance:", `Rs ${formatMoney(balanceAfterThis)}`);

//     // FOOTNOTE + SIGNATURES
//     y = Math.max(detailsY + 6, boxY + boxHeight + 6);

//     doc
//       .font("Helvetica")
//       .fontSize(8)
//       .text("* Dispute if any subject to Jamshedpur Jurisdiction", leftX, y, {
//         width: usableWidth,
//       });

//     const sigY = y + 40;
//     const sigWidth = 160;

//     doc
//       .moveTo(leftX, sigY)
//       .lineTo(leftX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", leftX, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX, sigY + 4, {
//         width: sigWidth,
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

// // ---------- PDF: Refund Receipt (A4 half page, professional layout) ----------
// app.get("/api/refunds/:id/refund-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid refund id" });

//   try {
//     const refundRef = db.collection("refunds").doc(id);
//     const refundSnap = await refundRef.get();
//     if (!refundSnap.exists) {
//       return res.status(404).json({ error: "Refund not found" });
//     }
//     const refund = refundSnap.data();
//     const billId = refund.billId;

//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();
//     const billTotal = Number(bill.total || 0);

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
//         return {
//           id: doc.id,
//           paymentDateTime,
//           amount: Number(d.amount || 0),
//         };
//       })
//       .sort((a, b) => {
//         const da = a.paymentDateTime
//           ? new Date(a.paymentDateTime)
//           : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//     const totalPaidGross = allPayments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const allRefunds = refundsSnap.docs
//       .map((doc) => {
//         const d = doc.data();
//         const refundDateTime =
//           d.refundDateTime ||
//           (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
//         return {
//           id: doc.id,
//           refundDateTime,
//           amount: Number(d.amount || 0),
//         };
//       })
//       .sort((a, b) => {
//         const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
//         const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
//         return da - dbb;
//       });

//     let cumulativeRefund = 0;
//     let refundedTillThis = 0;
//     let balanceAfterThis = billTotal;

//     for (const r of allRefunds) {
//       cumulativeRefund += r.amount;
//       if (r.id === id) {
//         refundedTillThis = cumulativeRefund;
//         const netPaidAfterThis = totalPaidGross - refundedTillThis;
//         balanceAfterThis = billTotal - netPaidAfterThis;
//         break;
//       }
//     }

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const patientName = bill.patientName || "";
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

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="refund-${id}.pdf"`);

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
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();
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
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 16;

//     // TITLE BAR
//     doc
//       .save()
//       .rect(36, y, usableWidth, 18)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 18)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("REFUND RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     // COMMON LAYOUT (left details + right summary)
//     const leftX = 36;
//     const rightBoxWidth = 180;
//     const rightX = pageWidth - 36 - rightBoxWidth;

//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Refund No.: ${refundNo}`, leftX, y);
//     doc.text(`Date: ${refund.refundDate || ""}`, rightX, y, {
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
//     addDetailR("Cheque Date:", chequeDate);
//     addDetailR("Bank:", bankName);
//     addDetailR("Transfer Type:", transferType);
//     addDetailR("Transfer Date:", transferDate);
//     addDetailR("UPI ID:", upiId);
//     addDetailR("UPI Name:", upiName);
//     addDetailR("UPI Date:", upiDate);

//     // RIGHT BILL SUMMARY
//     const boxY = detailsTopY;
//     const lineH = 12;
//     const rows = 6; // Bill No, Date, Total, Total Paid, Refunded, Balance
//     const boxHeight = 100;

//     doc.rect(rightX, boxY, rightBoxWidth, boxHeight).stroke();

//     let by2 = boxY + 4;
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(9)
//       .text("Bill Summary", rightX + 6, by2);
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
//     addRow2("Bill Date:", bill.date || "");
//     addRow2("Bill Total:", `Rs ${formatMoney(billTotal)}`);
//     addRow2("Total Paid:", `Rs ${formatMoney(totalPaidGross)}`);
//     addRow2("Refunded (incl. this):", `Rs ${formatMoney(refundedTillThis)}`);
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

//     doc
//       .moveTo(leftX, sigY)
//       .lineTo(leftX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", leftX, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX, sigY)
//       .lineTo(rightSigX + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX, sigY + 4, {
//         width: sigWidth,
//         align: "center",
//       });

//     doc.end();
//   } catch (err) {
//     console.error("refund-html-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate refund PDF" });
//     }
//   }
// });

// // ---------- PDF: Bill Summary (A4 half page with chronological table) ----------
// app.get("/api/bills/:id/summary-pdf", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     const billTotal = Number(bill.total || 0);

//     // --- PAYMENTS ---
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       const paymentDateTime =
//         d.paymentDateTime ||
//         (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         paymentDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         receiptNo: d.receiptNo || null,
//       };
//     });

//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // --- REFUNDS ---
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const refundDateTime =
//         d.refundDateTime ||
//         (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         refundDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         refundNo: d.refundReceiptNo || null,
//       };
//     });

//     refunds.sort((a, b) => {
//       const da = a.refundDateTime ?
//       new Date(a.refundDateTime) : new Date(0);
//       const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalRefunded = refunds.reduce(
//       (sum, r) => sum + Number(r.amount || 0),
//       0
//     );

//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = billTotal - netPaid;

//     const paymentsCount = payments.length;
//     const refundsCount = refunds.length;

//     const patientName = bill.patientName || "";
//     const invoiceNo = bill.invoiceNo || billId;
//     const billDate = bill.date || "";
//     const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     function formatDateTime(dtString) {
//       if (!dtString) return "";
//       const d = new Date(dtString);
//       if (Number.isNaN(d.getTime())) return "";
//       const yyyy = d.getFullYear();
//       const mm = String(d.getMonth() + 1).padStart(2, "0");
//       const dd = String(d.getDate()).padStart(2, "0");
//       const hh = String(d.getHours()).padStart(2, "0");
//       const mi = String(d.getMinutes()).padStart(2, "0");
//       return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
//     }

//     // --------- BUILD CHRONOLOGICAL TIMELINE ---------
//     const timeline = [];

//     const invoiceDateTime =
//       bill.createdAt || (bill.date ? `${bill.date}T00:00:00.000Z` : null);

//     timeline.push({
//       type: "INVOICE",
//       label: "Invoice Generated",
//       dateTime: invoiceDateTime,
//       mode: "-",
//       ref: invoiceNo,
//       debit: billTotal,
//       credit: 0,
//     });

//     payments.forEach((p) => {
//       timeline.push({
//         type: "PAYMENT",
//         label: p.receiptNo ? `Payment Receipt (${p.receiptNo})` : "Payment",
//         dateTime: p.paymentDateTime,
//         mode: p.mode || "",
//         ref: p.referenceNo || "",
//         debit: 0,
//         credit: p.amount,
//       });
//     });

//     refunds.forEach((r) => {
//       timeline.push({
//         type: "REFUND",
//         label: r.refundNo ? `Refund (${r.refundNo})` : "Refund",
//         dateTime: r.refundDateTime,
//         mode: r.mode || "",
//         ref: r.referenceNo || "",
//         debit: r.amount,
//         credit: 0,
//       });
//     });

//     timeline.sort((a, b) => {
//       const da = a.dateTime ? new Date(a.dateTime) : new Date(0);
//       const dbb = b.dateTime ? new Date(b.dateTime) : new Date(0);
//       return da - dbb;
//     });

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // ---------- PDF START ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="bill-summary-${billId}.pdf"`
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

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
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
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();
//     y += 6;

//     // static doctor header
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 16;

//     // title bar
//     doc
//       .save()
//       .rect(36, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 16)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("BILL SUMMARY", 36, y + 3, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 24;

//     // invoice / patient line
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${billDate}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     doc.text(`Patient Name: ${patientName}`, 36, y, {
//       width: usableWidth,
//     });

//     y += 18;

//     // --------- CHRONOLOGICAL TABLE ---------
//     const tableLeft = 36;
//     const colDateW = 80;
//     const colPartW = 150;
//     const colModeW = 60;
//     const colRefW = 80;
//     const colDebitW = 50;
//     const colCreditW = 50;
//     const colBalW =
//       usableWidth -
//       (colDateW + colPartW + colModeW + colRefW + colDebitW + colCreditW);

//     const colDateX = tableLeft;
//     const colPartX = colDateX + colDateW;
//     const colModeX = colPartX + colPartW;
//     const colRefX = colModeX + colModeW;
//     const colDebitX = colRefX + colRefW;
//     const colCreditX = colDebitX + colDebitW;
//     const colBalX = colCreditX + colCreditW;

//     // header background
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, 16)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(8);
//     doc.text("Date & Time", colDateX + 2, y + 3, {
//       width: colDateW - 4,
//     });
//     doc.text("Particulars", colPartX + 2, y + 3, {
//       width: colPartW - 4,
//     });
//     doc.text("Mode", colModeX + 2, y + 3, {
//       width: colModeW - 4,
//     });
//     doc.text("Reference", colRefX + 2, y + 3, {
//       width: colRefW - 4,
//     });
//     doc.text("Debit (Rs)", colDebitX + 2, y + 3, {
//       width: colDebitW - 4,
//       align: "right",
//     });
//     doc.text("Credit (Rs)", colCreditX + 2, y + 3, {
//       width: colCreditW - 4,
//       align: "right",
//     });
//     doc.text("Balance (Rs)", colBalX + 2, y + 3, {
//       width: colBalW - 4,
//       align: "right",
//     });

//     y += 16;
//     doc.font("Helvetica").fontSize(8);

//     let runningBalance = 0;

//     timeline.forEach((ev) => {
//       const rowHeight = 14;

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       if (ev.type === "INVOICE") {
//         runningBalance = ev.debit - ev.credit;
//       } else {
//         runningBalance += ev.debit;
//         runningBalance -= ev.credit;
//       }

//       doc.text(formatDateTime(ev.dateTime), colDateX + 2, y + 3, {
//         width: colDateW - 4,
//       });
//       doc.text(ev.label || "", colPartX + 2, y + 3, {
//         width: colPartW - 4,
//       });
//       doc.text(ev.mode || "", colModeX + 2, y + 3, {
//         width: colModeW - 4,
//       });
//       doc.text(ev.ref || "", colRefX + 2, y + 3, {
//         width: colRefW - 4,
//       });
//       doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 2, y + 3, {
//         width: colDebitW - 4,
//         align: "right",
//       });
//       doc.text(ev.credit ? formatMoney(ev.credit) : "", colCreditX + 2, y + 3, {
//         width: colCreditW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(runningBalance), colBalX + 2, y + 3, {
//         width: colBalW - 4,
//         align: "right",
//       });

//       y += rowHeight;
//     });

//     y += 18;

//     // --------- TOTALS BOX ---------
//     const boxWidth = 260;
//     const boxX = 36;
//     const boxY = y;
//     const lineH2 = 12;
//     const rows2 = 8;
//     const boxHeight = lineH2 * rows2 + 8;

//     doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

//     let by3 = boxY + 4;

//     doc.font("Helvetica").fontSize(9);

//     function row(label, value) {
//       doc.text(label, boxX + 6, by3);
//       doc.text(value, boxX + 6, by3, {
//         width: boxWidth - 12,
//         align: "right",
//       });
//       by3 += lineH2;
//     }

//     row("Bill Total", `Rs ${formatMoney(billTotal)}`);
//     row("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     row("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     row("Net Paid", `Rs ${formatMoney(netPaid)}`);
//     row("Balance", `Rs ${formatMoney(balance)}`);
//     row("Payments Count", String(paymentsCount));
//     row("Refunds Count", String(refundsCount));
//     row("Status", status);

//     const rightSigWidth = 160;
//     const sigY2 = boxY + boxHeight + 30;
//     const rightSigX = pageWidth - 36 - rightSigWidth;

//     doc
//       .moveTo(rightSigX, sigY2)
//       .lineTo(rightSigX + rightSigWidth, sigY2)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX, sigY2 + 4, {
//         width: rightSigWidth,
//         align: "center",
//       });

//     doc.end();
//   } catch (err) {
//     console.error("summary-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate summary PDF" });
//     }
//   }
// });

// // ---------- PUT /api/bills/:id (edit bill: patient + services, NOT payments) ----------
// app.put("/api/bills/:id", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();

//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const oldBill = billSnap.data();

//     // Editable fields from frontend (everything except payment info)
//     const { patientName, sex, address, age, date, adjust, remarks, services } =
//       req.body;

//     const jsDate =
//       date || oldBill.date || new Date().toISOString().slice(0, 10);

//     // --- NORMALIZE SERVICES (same style as POST /api/bills) ---
//     const normalizedServices = Array.isArray(services)
//       ? services.map((s) => {
//           const qty = Number(s.qty) || 0;
//           const rate = Number(s.rate) || 0;
//           const amount = qty * rate;
//           return {
//             item: s.item || "",
//             details: s.details || "",
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     // YE data hum ITEMS collection me likhenge
//     const itemsData = normalizedServices.map((s) => {
//       const parts = [];
//       if (s.item) parts.push(s.item);
//       if (s.details) parts.push(s.details);
//       const description = parts.join(" - ") || "";
//       return {
//         description,
//         qty: s.qty,
//         rate: s.rate,
//         amount: s.amount,
//       };
//     });

//     const subtotal = itemsData.reduce(
//       (sum, it) => sum + Number(it.amount || 0),
//       0
//     );
//     const adj = Number(adjust ?? oldBill.adjust ?? 0) || 0;
//     const total = subtotal + adj;

//     // keep payments/refunds as is
//     const paidGross = Number(oldBill.paid || 0);
//     const refunded = Number(oldBill.refunded || 0);
//     const effectivePaid = paidGross - refunded;
//     const balance = total - effectivePaid;
//     const status = computeStatus(total, effectivePaid);

//     const batch = db.batch();

//     // 1) Update bill doc
//     const finalPatientName = patientName ?? oldBill.patientName ?? "";

//     batch.update(billRef, {
//       patientName: finalPatientName,
//       sex: sex ?? oldBill.sex ?? null,
//       address: address ?? oldBill.address ?? "",
//       age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
//       date: jsDate,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: paidGross,
//       refunded,
//       balance,
//       status,
//       remarks:
//         typeof remarks !== "undefined" ? remarks : oldBill.remarks ?? null,
//       services: normalizedServices,
//     });

//     // 2) Replace items collection for this bill
//     //    (pehle saare old items delete, fir naya set create)
//     const existingItemsSnap = await db
//       .collection("items")
//       .where("billId", "==", billId)
//       .get();

//     existingItemsSnap.forEach((doc) => {
//       batch.delete(doc.ref);
//     });

//     // 3) NEW ITEMS INSERT with deterministic ID:
//     itemsData.forEach((item, index) => {
//       const lineNo = index + 1;
//       const itemId = `${billId}-${String(lineNo).padStart(2, "0")}`; // e.g. 25-26_INV-0001-01

//       const qty = Number(item.qty || 0);
//       const rate = Number(item.rate || 0);
//       const amount = Number(item.amount || qty * rate || 0);

//       batch.set(db.collection("items").doc(itemId), {
//         billId,
//         patientName: finalPatientName,
//         description: item.description,
//         qty,
//         rate,
//         amount,
//       });
//     });

//     await batch.commit();

//     // clear caches so GET /api/bills and /api/bills/:id show updated data
//     cache.flushAll();

//     // update Google Sheet (optional but consistent with create)
//     syncBillToSheet({
//       id: billId,
//       invoiceNo: oldBill.invoiceNo || billId,
//       patientName: finalPatientName,
//       address: address ?? oldBill.address ?? "",
//       age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
//       date: jsDate,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: paidGross,
//       refunded,
//       balance,
//       status,
//       sex: sex ?? oldBill.sex ?? null,
//     });

//     syncItemsToSheet(
//       billId,
//       billId,
//       finalPatientName,
//       itemsData.map((it) => ({
//         description: it.description,
//         qty: it.qty,
//         rate: it.rate,
//         amount: it.amount,
//       }))
//     );

//     res.json({
//       id: billId,
//       invoiceNo: oldBill.invoiceNo || billId,
//       patientName: finalPatientName,
//       sex: sex ?? oldBill.sex ?? null,
//       address: address ?? oldBill.address ?? "",
//       age: typeof age !== "undefined" ? Number(age) : oldBill.age ?? null,
//       date: jsDate,
//       subtotal,
//       adjust: adj,
//       total,
//       paid: paidGross,
//       refunded,
//       balance,
//       status,
//       remarks:
//         typeof remarks !== "undefined" ? remarks : oldBill.remarks ?? null,
//       services: normalizedServices,
//     });
//   } catch (err) {
//     console.error("PUT /api/bills/:id error:", err);
//     res.status(500).json({ error: "Failed to update bill" });
//   }
// });

// // DELETE bill + items + payments + refunds + sheet rows
// app.delete("/api/bills/:id", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();

//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }

//     const bill = billSnap.data();
//     const invoiceNo = bill.invoiceNo || billId;

//     // ---- FETCH CHILD DOCUMENTS ----
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", billId)
//       .get();
//     const paymentsSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     // ---- BATCH DELETE ----
//     const batch = db.batch();

//     batch.delete(billRef);

//     itemsSnap.forEach((doc) => batch.delete(doc.ref));
//     paymentsSnap.forEach((doc) => batch.delete(doc.ref));
//     refundsSnap.forEach((doc) => batch.delete(doc.ref));

//     await batch.commit();

//     // ---- CLEAR CACHE ----
//     cache.flushAll();

//     // ---- GOOGLE SHEETS DELETE ----
//     syncDeleteBillFromSheet(invoiceNo);
//     syncDeleteItemsFromSheet(billId);
//     syncDeletePaymentsFromSheet(billId);
//     syncDeleteRefundsFromSheet(billId);

//     return res.json({
//       success: true,
//       message: "Bill, Items, Payments, Refunds deleted successfully",
//       deleted: {
//         billId,
//         items: itemsSnap.size,
//         payments: paymentsSnap.size,
//         refunds: refundsSnap.size,
//       },
//     });
//   } catch (err) {
//     console.error("DELETE /api/bills/:id error:", err);
//     return res.status(500).json({ error: "Failed to delete bill" });
//   }
// });






// app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   function formatMoney(v) { return Number(v || 0).toFixed(2); }

//   function formatDateOnly(dtString) {
//     if (!dtString) return "";
//     const d = new Date(dtString);
//     if (Number.isNaN(d.getTime())) {
//       // try parse common yyyy-mm-dd or dd/mm/yyyy input fallbacks
//       // if dtString contains '-' assume yyyy-mm-dd
//       if (/^\d{4}-\d{2}-\d{2}/.test(dtString)) {
//         const parts = dtString.split("T")[0].split("-");
//         if (parts.length >= 3) return `${parts[2].padStart(2,"0")}/${parts[1].padStart(2,"0")}/${parts[0]}`;
//       }
//       // if already dd/mm/yyyy
//       if (/^\d{2}\/\d{2}\/\d{4}$/.test(dtString)) return dtString;
//       return dtString || "";
//     }
//     const dd = String(d.getDate()).padStart(2, "0");
//     const mm = String(d.getMonth() + 1).padStart(2, "0");
//     const yyyy = d.getFullYear();
//     return `${dd}/${mm}/${yyyy}`;
//   }

//   try {
//     // load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
//     const bill = billSnap.data();

//     // fetch items (legacy/new combined)
//     const itemsSnap = await db.collection("items").where("billId", "==", id).get();
//     const legacyItems = itemsSnap.docs.map((d) => {
//       const dd = d.data();
//       return {
//         id: d.id,
//         description: dd.description || dd.item || dd.details || "",
//         qty: Number(dd.qty || 0),
//         rate: Number(dd.rate || 0),
//         amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
//       };
//     });

//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;
//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);
//           return {
//             id: `svc-${idx + 1}`,
//             description: parts.join(" - "),
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // payments & refunds
//     const paysSnap = await db.collection("payments").where("billId", "==", id).get();
//     const payments = paysSnap.docs.map((d) => {
//       const pd = d.data();
//       return {
//         id: d.id,
//         paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
//         paymentDate: pd.paymentDate || null,
//         paymentTime: pd.paymentTime || null,
//         amount: Number(pd.amount || 0),
//         mode: pd.mode || "",
//         referenceNo: pd.referenceNo || "",
//         chequeDate: pd.chequeDate || null,
//         chequeNumber: pd.chequeNumber || null,
//         bankName: pd.bankName || null,
//         transferType: pd.transferType || null,
//         transferDate: pd.transferDate || null,
//         upiName: pd.upiName || null,
//         upiId: pd.upiId || null,
//         upiDate: pd.upiDate || null,
//         drawnOn: pd.drawnOn || null,
//         drawnAs: pd.drawnAs || null,
//         receiptNo: pd.receiptNo || d.id,
//       };
//     });

//     const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
//     const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

//     // sort payments chronologically
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     // totals
//     const subtotal = Number(bill.subtotal || items.reduce((s, it) => s + Number(it.amount || 0), 0));
//     const adjust = Number(bill.adjust || 0);
//     const total = Number(bill.total || subtotal + adjust || 0);
//     const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
//     const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = total - netPaid;

//     // Only allow full-payment PDF if balance is zero (or less)
//     if (balance > 0) {
//       return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
//     }

//     // ---------- FETCH CLINIC PROFILE ----------
//     // const profile = await getClinicProfile({ force: true });
//     // const clinicName = profileValue(profile, "clinicName") || "MADHUREKHA EYE CARE CENTRE";
//     // const clinicAddress = profileValue(profile, "address") || "SONARI: E-501, Sonari East Layout, Near Sabuz Sangh Kali Puja Maidan, Jamshedpur - 831011";
//     // const clinicPAN = profileValue(profile, "pan") || "ABFFM3115J";
//     // const clinicRegNo = profileValue(profile, "regNo") || "2035700023";
//     // const patientRepresentative = profileValue(profile, "patientRepresentative") || "Patient / Representative";
//     // const clinicRepresentative = profileValue(profile, "clinicRepresentative") || "For Madhurekha Eye Care Centre";
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");


//     // --- PDF Setup ---
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

//     const doc = new PDFDocument({ size: "A4", margin: 36 });
//     doc.pipe(res);

//     // register local font if available (optional)
//     try {
//       const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
//       if (fs && fs.existsSync(workSansPath)) {
//         doc.registerFont("WorkSans", workSansPath);
//         doc.font("WorkSans");
//       } else {
//         doc.font("Helvetica");
//       }
//     } catch (e) {
//       doc.font("Helvetica");
//     }

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     // Header logos (try-catch because resources may not exist)
//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
//     try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//     try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

//     // Clinic header (from profile)
//     doc.fontSize(14).font("Helvetica-Bold").text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//     doc.fontSize(9).font("Helvetica").text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//     doc.text(`PAN : ${clinicPAN}   |   Reg. No: ${clinicRegNo}`, { align: "center", width: pageWidth });

//     y += 56;
//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 8;

//     // Invoice Title
//     doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//     doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//     y += 28;

//     // Invoice details line (Invoice No + Date) - date only DD/MM/YYYY
//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateOnly(bill.date || "");
//     doc.fontSize(9).font("Helvetica");
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     // Patient info (address & sex swapped)
//     const patientName = bill.patientName || "";
//     const sexText = bill.sex ? String(bill.sex) : "";
//     const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const addressText = bill.address || "";

//     doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
//     doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
//     doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 20;

//     // ---------- SERVICES / ITEMS TABLE ----------
//     const tableLeft = 36;
//     const colSrW = 24;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colAdjW = 60;
//     const colSubW = 80;
//     const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

//     let xSr = tableLeft;
//     let xQty = xSr + colSrW;
//     let xService = xQty + colQtyW;
//     let xRate = xService + colServiceW;
//     let xAdj = xRate + colRateW;
//     let xSub = xAdj + colAdjW;

//     // header
//     doc.save().rect(tableLeft, y, usableWidth, 16).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, 16).stroke();
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", xSr + 2, y + 3);
//     doc.text("Qty", xQty + 2, y + 3);
//     doc.text("Procedure", xService + 2, y + 3, { width: colServiceW - 4 });
//     doc.text("Rate / Price", xRate + 2, y + 3, { width: colRateW - 4, align: "right" });
//     doc.text("Adjust", xAdj + 2, y + 3, { width: colAdjW - 4, align: "right" });
//     doc.text("Amount", xSub + 2, y + 3, { width: colSubW - 4, align: "right" });
//     y += 16;

//     // rows (dynamic height)
//     doc.font("Helvetica").fontSize(9);
//     const svcMinRowH = 14;
//     for (let i = 0; i < items.length; i++) {
//       const it = items[i];
//       const rowPadding = 6;

//       const descH = doc.heightOfString(it.description || "", { width: colServiceW - 4 });
//       const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 4 });
//       const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 4 });
//       const amountH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 4 });

//       const rowTextMaxH = Math.max(descH, qtyH, rateH, amountH);
//       const rowHeight = Math.max(svcMinRowH, rowTextMaxH + rowPadding);

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       const descY = y + (rowHeight - descH) / 2;
//       const qtyY = y + (rowHeight - qtyH) / 2;
//       const rateY = y + (rowHeight - rateH) / 2;
//       const amountY = y + (rowHeight - amountH) / 2;

//       doc.text(String(i + 1), xSr + 2, y + 3);
//       doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), xQty + 2, qtyY, { width: colQtyW - 4, align: "left" });
//       doc.text(it.description || "", xService + 2, descY, { width: colServiceW - 4 });
//       doc.text(formatMoney(it.rate || 0), xRate + 2, rateY, { width: colRateW - 4, align: "right" });
//       doc.text("0.00", xAdj + 2, y + 3, { width: colAdjW - 4, align: "right" });
//       doc.text(formatMoney(it.amount || 0), xSub + 2, amountY, { width: colSubW - 4, align: "right" });

//       y += rowHeight;

//       if (y > doc.page.height - 160) {
//         doc.addPage();
//         y = 36;
//       }
//     }

//     y += 12;

//     // ---------- PAYMENT DETAILS (chronological) ----------
//     const pTableLeft = 36;
//     const pColDateW = 100; // reduced width since time removed
//     const pColRecW = 140;
//     const pColModeW = 90;
//     const pColBankW = 110;
//     const pColRefW = 110;
//     const pColAmtW = usableWidth - (pColDateW + pColRecW + pColModeW + pColBankW + pColRefW);

//     const pColDateX = pTableLeft;
//     const pColRecX = pColDateX + pColDateW;
//     const pColModeX = pColRecX + pColRecW;
//     const pColBankX = pColModeX + pColModeW;
//     const pColRefX = pColBankX + pColBankW;
//     const pColAmtX = pColRefX + pColRefW;

//     // header
//     doc.save().rect(pTableLeft, y, usableWidth, 18).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, 18).stroke();
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//     doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//     doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//     doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//     doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//     doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
//     y += 18;

//     const pMinRowH = 16;
//     for (const p of payments) {
//       const dateTextP = formatDateOnly(p.paymentDateTime || p.paymentDate || `${p.paymentDate || ""}`);
//       const receiptText = p.receiptNo || p.id || "";
//       let modeText = p.mode || "-";
//       if ((modeText === "BankTransfer" || modeText.toLowerCase().includes("bank")) && p.transferType) {
//         modeText = `Bank (${p.transferType})`;
//       }
//       const bankText = p.bankName || "-";
//       const refText = p.referenceNo || "-";
//       const amtText = formatMoney(p.amount || 0);

//       doc.font("Helvetica").fontSize(9);
//       const dH = doc.heightOfString(dateTextP, { width: pColDateW - 8 });
//       const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
//       const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
//       const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
//       const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
//       const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

//       const maxH = Math.max(dH, rH, mH, bH, refH, aH);
//       const rowH = Math.max(pMinRowH, maxH + 8);

//       doc.rect(pTableLeft, y, usableWidth, rowH).stroke();

//       const dateY = y + (rowH - dH) / 2;
//       const recY = y + (rowH - rH) / 2;
//       const modeY = y + (rowH - mH) / 2;
//       const bankY = y + (rowH - bH) / 2;
//       const refY = y + (rowH - refH) / 2;
//       const amtY = y + (rowH - aH) / 2;

//       doc.text(dateTextP, pColDateX + 4, dateY, { width: pColDateW - 8 });
//       doc.text(receiptText, pColRecX + 4, recY, { width: pColRecW - 8 });
//       doc.text(modeText, pColModeX + 4, modeY, { width: pColModeW - 8 });
//       doc.text(bankText, pColBankX + 4, bankY, { width: pColBankW - 8 });
//       doc.text(refText, pColRefX + 4, refY, { width: pColRefW - 8 });
//       doc.text(amtText, pColAmtX + 4, amtY, { width: pColAmtW - 8, align: "right" });

//       y += rowH;

//       if (y > doc.page.height - 140) {
//         doc.addPage();
//         y = 36;
//         // redraw payments header on new page
//         doc.save().rect(pTableLeft, y, usableWidth, 18).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, 18).stroke();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//         doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//         doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//         doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//         doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//         doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
//         y += 18;
//       }
//     }

//     y += 12;

//     // ---------- TOTALS BOX (after payment table) ----------
//     const boxWidth = 260;
//     const boxX = 36;
//     const boxY = y;
//     const lineH = 12;
//     const rowsCount = 5;
//     const boxHeight = rowsCount * lineH + 8;

//     doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();
//     let by = boxY + 6;
//     doc.font("Helvetica").fontSize(9);

//     const addRow = (label, value) => {
//       doc.text(label, boxX + 6, by);
//       doc.text(value, boxX + 6, by, { width: boxWidth - 12, align: "right" });
//       by += lineH;
//     };

//     addRow("Sub Total", `Rs ${formatMoney(subtotal)}`);
//     addRow("Adjust", `Rs ${formatMoney(adjust)}`);
//     addRow("Total", `Rs ${formatMoney(total)}`);
//     addRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     addRow("Net Paid (after refunds)", `Rs ${formatMoney(netPaid)}`);

//     y = boxY + boxHeight + 20;

//     // footer note + signatures
//     doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
//     const sigY = y + 28;
//     const sigWidth = 160;
//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative, 36, sigY + 4, { width: sigWidth, align: "center" });
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative, rightSigX, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("full-payment-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate full payment PDF" });
//     } else {
//       try { res.end(); } catch (e) {}
//     }
//   }
// });




// app.get("/api/profile", async (_req, res) => {
//   try {
//     const key = makeCacheKey("profile", "clinic");
//     const data = await getOrSetCache(key, 120, async () => {
//       const profileRef = db.collection("settings").doc("clinicProfile");
//       const profileSnap = await profileRef.get();

//       if (!profileSnap.exists) {
//         // Return null if profile doesn't exist (first time setup)
//         return { exists: false };
//       }

//       return { exists: true, ...profileSnap.data() };
//     });

//     res.json(data);
//   } catch (err) {
//     console.error("GET /api/profile error:", err);
//     res.status(500).json({ error: "Failed to fetch profile" });
//   }
// });

// // ---------- PUT /api/profile (update clinic profile) ----------
// app.put("/api/profile", async (req, res) => {
//   try {
//     const {
//       clinicName,
//       address,
//       pan,
//       regNo,
//       doctor1Name,
//       doctor1RegNo,
//       doctor2Name,
//       doctor2RegNo,
//       patientRepresentative,
//       clinicRepresentative,
//       phone,
//       email,
//       website,
//     } = req.body;

//     const profileData = {
//       clinicName: clinicName || "",
//       address: address || "",
//       pan: pan || "",
//       regNo: regNo || "",
//       doctor1Name: doctor1Name || "",
//       doctor1RegNo: doctor1RegNo || "",
//       doctor2Name: doctor2Name || "",
//       doctor2RegNo: doctor2RegNo || "",
//       patientRepresentative: patientRepresentative || "",
//       clinicRepresentative: clinicRepresentative || "",
//       phone: phone || "",
//       email: email || "",
//       website: website || "",
//       updatedAt: new Date().toISOString(),
//     };

//     const profileRef = db.collection("settings").doc("clinicProfile");
//     await profileRef.set(profileData, { merge: true });

//     // Clear cache
//     cache.flushAll();

//     // Sync to Google Sheet (optional - add this function to sheetIntregation.js)
//     try {
//       syncProfileToSheet(profileData);
//     } catch (sheetErr) {
//       console.warn("Sheet sync failed for profile:", sheetErr);
//     }

//     res.json({
//       success: true,
//       message: "Profile updated successfully",
//       profile: profileData,
//     });
//   } catch (err) {
//     console.error("PUT /api/profile error:", err);
//     res.status(500).json({ error: "Failed to update profile" });
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
import PDFDocument from "pdfkit";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { db } from "./firebaseClient.js";
// at top of server.js
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
  const d = typeof dateStrOrDate === "string" ? new Date(dateStrOrDate) : dateStrOrDate;
  if (Number.isNaN(d.getTime && d.getTime())) {
    // try to parse common yyyy-mm-dd or iso fragments
    if (typeof dateStrOrDate === "string") {
      const s = dateStrOrDate.split("T")[0];
      if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
        const parts = s.split("-");
        return `${parts[2].padStart(2, "0")}.${parts[1].padStart(2, "0")}.${parts[0]}`;
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

// --------- ID HELPERS (FINANCIAL YEAR + SEQUENCES, NO COUNTERS COLLECTION) ----------

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

// Generate invoice number WITHOUT counters collection
async function generateInvoiceNumber(billDateInput) {
  const dateStr = billDateInput || new Date().toISOString().slice(0, 10);
  const fy = getFinancialYearCode(dateStr);
  const prefix = `${fy}/INV-`;

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
  const invoiceNo = `${fy}/INV-${serial}`;

  return { invoiceNo, fy, serial };
}

// Parse "25-26/INV-0001" into { fy: "25-26", invoiceSerial: "0001" }
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
  return `${fy}/INV-${invoiceSerial}/REC-${recSerial}`;
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
  return `${fy}/INV-${invoiceSerial}/REF-${refSerial}`;
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
          date: formatDateDot(b.date || null),
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

      // service rows from CreateBill (frontend sends description)
      services,
    } = req.body;

    // store date as ISO yyyy-mm-dd for queries, but display as DD.MM.YYYY in outputs
    const jsDateISO = date || new Date().toISOString().slice(0, 10);

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
          date: formatDateDot(d.paymentDate || null),
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
          date: formatDateDot(d.refundDate || null),
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
        date: formatDateDot(bill.date || null),
        total,
        paid: netPaid,
        refunded: totalRefunded,
        totalPaid: totalPaidGross,
        balance,
        status,
        // doctorReg1 and doctorReg2 intentionally omitted (static in PDFs only)
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
    const paymentDate = now.toISOString().slice(0, 10); // stored as yyyy-mm-dd
    const paymentTime = now.toTimeString().slice(0, 5);
    const paymentDateTime = now.toISOString();

    const invoiceNo = bill.invoiceNo || billId;
    const receiptNo = await generateReceiptId(invoiceNo, billId); // pass billId
    const paymentId = receiptNo.replace(/\//g, "_");
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
      paymentDateFormatted: formatDateDot(paymentDate),
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
    const refundReceiptNo = await generateRefundId(invoiceNo, billId); // pass billId
    const refundId = refundReceiptNo.replace(/\//g, "_");
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
      refundDateFormatted: formatDateDot(refundDate),
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
        paymentDate: formatDateDot(payment.paymentDate),
        receiptNo: payment.receiptNo || `R-${String(id).padStart(4, "0")}`,
        bill: {
          id: billId,
          date: formatDateDot(bill.date),
          total: billTotal,
          paid: paidTillThis,
          balance: balanceAfterThis,
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
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//         const da = a.paymentDateTime
//           ? new Date(a.paymentDateTime)
//           : new Date(0);
//         const dbb = b.paymentDateTime
//           ? new Date(b.paymentDateTime)
//           : new Date(0);
//         return da - dbb;
//       });

//     const primaryPayment = payments[0] || null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateDot(bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colAdjW = 60; // kept for layout but not used as "Adjust"
//     const colSubW = 70;
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

//     const colSrX = tableLeft;
//     const colQtyX = colSrX + colSrW;
//     const colServiceX = colQtyX + colQtyW;
//     const colRateX = colServiceX + colServiceW;
//     const colAdjX = colRateX + colRateW;
//     const colSubX = colAdjX + colAdjW;

//     // header background
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, 16)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     // Adjust column intentionally left blank (removed)
//     doc.text("Amount", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += 16;
//     doc.font("Helvetica").fontSize(9);

//     items.forEach((item, idx) => {
//       const rowHeight = 14;

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       y += rowHeight;

//       if (y > doc.page.height - 200) {
//         doc.addPage();
//         y = 36;
//       }
//     });

//     y += 12;

//     // 9) TOTALS BOX
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     // We removed Sub Total, Adjust, Tax — show Refunded and Total Due only (keeps layout compact)
//     doc.rect(boxX, boxY, boxWidth, lineH * 2 + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     doc.text("Refunded", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(totalRefunded)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2 + lineH);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2 + lineH, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // move below totals box
//     y = boxY + lineH * 2 + 20;

//     // NET PAID + BALANCE
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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

// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS (services take precedence)
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//       // sort by date ASC so earliest (first) payment is first in array
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     // primaryPayment = first (earliest) payment (explicit)
//     const primaryPayment = payments.length > 0 ? payments[0] : null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS (still used for math but not printed as a separate line)
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     // if bill.total is not present, compute from items (so adding items updates invoice)
//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // ensure invoice no is fixed once generated: if missing, generate and save back
//     const generatedInvoiceNo = `INV-${id}`;
//     const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
//     if (!bill.invoiceNo) {
//       // best-effort: persist invoice number so next time invoice remains same
//       try {
//         await billRef.update({ invoiceNo });
//       } catch (e) {
//         // non-fatal: continue even if update fails (e.g., permission)
//         console.warn("Failed to persist invoiceNo:", e);
//       }
//     }

//     const dateText = formatDateDot ? formatDateDot(bill.date || "") : (bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 70; // amount
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200; // change this value to adjust fixed visual table height

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Amount", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += headerHeight;
//     doc.font("Helvetica").fontSize(9);

//     // draw actual rows; allow them to flow to new pages naturally
//     items.forEach((item, idx) => {
//       // if not enough space for one row + footer area, add page
//       if (y + rowHeight > doc.page.height - 120) {
//         doc.addPage();
//         y = 36;

//         // redraw header on new page
//         doc
//           .save()
//           .rect(tableLeft, y, usableWidth, headerHeight)
//           .fill("#F3F3F3")
//           .restore()
//           .rect(tableLeft, y, usableWidth, headerHeight)
//           .stroke();

//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//           width: colServiceW - 4,
//         });
//         doc.text("Qty", colQtyX + 2, y + 3);
//         doc.text("Rate", colRateX + 2, y + 3, {
//           width: colRateW - 4,
//           align: "right",
//         });
//         doc.text("Amount", colSubX + 2, y + 3, {
//           width: colSubW - 4,
//           align: "right",
//         });

//         y += headerHeight;
//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row border and content
//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       y += rowHeight;
//     });

//     // After drawing items, enforce a minimum visual table height by drawing empty rows
//     // so the table looks fixed when few rows exist.
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       const remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);

//       for (let i = 0; i < emptyRows; i++) {
//         // if not enough space for another empty row, add page and redraw header
//         if (y + rowHeight > doc.page.height - 120) {
//           doc.addPage();
//           y = 36;

//           // redraw header on new page
//           doc
//             .save()
//             .rect(tableLeft, y, usableWidth, headerHeight)
//             .fill("#F3F3F3")
//             .restore()
//             .rect(tableLeft, y, usableWidth, headerHeight)
//             .stroke();

//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//             width: colServiceW - 4,
//           });
//           doc.text("Qty", colQtyX + 2, y + 3);
//           doc.text("Rate", colRateX + 2, y + 3, {
//             width: colRateW - 4,
//             align: "right",
//           });
//           doc.text("Amount", colSubX + 2, y + 3, {
//             width: colSubW - 4,
//             align: "right",
//           });

//           y += headerHeight;
//           doc.font("Helvetica").fontSize(9);
//         }

//         // draw empty row box
//         doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();
//         y += rowHeight;
//       }
//     }

//     // small gap after table
//     y += 12;

//     // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Only show Total Due (compact)
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // move below totals box
//     y = boxY + lineH + 20;

//     // NET PAID + BALANCE (these remain)
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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
// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS (services take precedence)
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//       // sort by date ASC so earliest (first) payment is first in array
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     // primaryPayment = first (earliest) payment (explicit)
//     const primaryPayment = payments.length > 0 ? payments[0] : null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS (still used for math but not printed as a separate line)
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     // if bill.total is not present, compute from items (so adding items updates invoice)
//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // ensure invoice no is fixed once generated: if missing, generate and save back
//     const generatedInvoiceNo = `INV-${id}`;
//     const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
//     if (!bill.invoiceNo) {
//       // best-effort: persist invoice number so next time invoice remains same
//       try {
//         await billRef.update({ invoiceNo });
//       } catch (e) {
//         // non-fatal: continue even if update fails (e.g., permission)
//         console.warn("Failed to persist invoiceNo:", e);
//       }
//     }

//     const dateText = typeof formatDateDot === "function" ? formatDateDot(bill.date || "") : (bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 70; // amount
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200; // change this value to adjust fixed visual table height
//     const bottomSafety = 120; // reserved area to avoid overlapping footer

//     // helper to draw vertical separators for table from yTop to yTop+height
//     function drawVerticals(yTop, height) {
//       const top = yTop;
//       const bottom = yTop + height;
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     // draw vertical separators for header
//     drawVerticals(y, headerHeight);

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Amount", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += headerHeight;
//     doc.font("Helvetica").fontSize(9);

//     // draw actual rows; allow them to flow to new pages naturally
//     items.forEach((item, idx) => {
//       // if not enough space for one row + footer area, add page
//       if (y + rowHeight > doc.page.height - bottomSafety) {
//         doc.addPage();
//         y = 36;

//         // redraw header on new page
//         doc
//           .save()
//           .rect(tableLeft, y, usableWidth, headerHeight)
//           .fill("#F3F3F3")
//           .restore()
//           .rect(tableLeft, y, usableWidth, headerHeight)
//           .stroke();

//         drawVerticals(y, headerHeight);

//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//           width: colServiceW - 4,
//         });
//         doc.text("Qty", colQtyX + 2, y + 3);
//         doc.text("Rate", colRateX + 2, y + 3, {
//           width: colRateW - 4,
//           align: "right",
//         });
//         doc.text("Amount", colSubX + 2, y + 3, {
//           width: colSubW - 4,
//           align: "right",
//         });

//         y += headerHeight;
//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row border and content
//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();
//       // draw vertical separators for this row
//       drawVerticals(y, rowHeight);

//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       y += rowHeight;
//     });

//     // After drawing items, enforce a minimum visual table height by drawing empty rows
//     // so the table looks fixed when few rows exist.
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       const remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);

//       for (let i = 0; i < emptyRows; i++) {
//         // if not enough space for another empty row, add page and redraw header
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           doc.addPage();
//           y = 36;

//           // redraw header on new page
//           doc
//             .save()
//             .rect(tableLeft, y, usableWidth, headerHeight)
//             .fill("#F3F3F3")
//             .restore()
//             .rect(tableLeft, y, usableWidth, headerHeight)
//             .stroke();

//           drawVerticals(y, headerHeight);

//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//             width: colServiceW - 4,
//           });
//           doc.text("Qty", colQtyX + 2, y + 3);
//           doc.text("Rate", colRateX + 2, y + 3, {
//             width: colRateW - 4,
//             align: "right",
//           });
//           doc.text("Amount", colSubX + 2, y + 3, {
//             width: colSubW - 4,
//             align: "right",
//           });

//           y += headerHeight;
//           doc.font("Helvetica").fontSize(9);
//         }

//         // draw empty row box + vertical separators
//         doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();
//         drawVerticals(y, rowHeight);
//         y += rowHeight;
//       }
//     }

//     // small gap after table
//     y += 12;

//     // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Only show Total Due (compact)
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // move below totals box
//     y = boxY + lineH + 20;

//     // NET PAID + BALANCE (these remain)
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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


// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS (services take precedence)
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//       // sort by date ASC so earliest (first) payment is first in array
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     // primaryPayment = first (earliest) payment (explicit)
//     const primaryPayment = payments.length > 0 ? payments[0] : null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS (still used for math but not printed as a separate line)
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     // if bill.total is not present, compute from items (so adding items updates invoice)
//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // ensure invoice no is fixed once generated: if missing, generate and save back
//     const generatedInvoiceNo = `INV-${id}`;
//     const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
//     if (!bill.invoiceNo) {
//       // best-effort: persist invoice number so next time invoice remains same
//       try {
//         await billRef.update({ invoiceNo });
//       } catch (e) {
//         // non-fatal: continue even if update fails (e.g., permission)
//         console.warn("Failed to persist invoiceNo:", e);
//       }
//     }

//     const dateText = typeof formatDateDot === "function" ? formatDateDot(bill.date || "") : (bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 70; // amount
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200; // change this value to adjust fixed visual table height
//     const bottomSafety = 120; // reserved area to avoid overlapping footer

//     // We'll draw only one horizontal line after the header.
//     // We will NOT draw horizontal borders per row.
//     // Vertical separators will be drawn for the whole content block per page segment.

//     // helper to draw vertical separators for a vertical segment (on current page)
//     function drawVerticalsForSegment(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Amount", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += headerHeight;

//     // draw a single horizontal separator line just below the header (one horizontal row after heading)
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // We will accumulate a single vertical segment per page.
//     let segmentStartY = y; // start of content area for current page
//     let segmentHeight = 0; // how tall content area grows on current page

//     doc.font("Helvetica").fontSize(9);

//     // iterate items and draw content (no horizontal borders). Manage page breaks and vertical segments.
//     for (let idx = 0; idx < items.length; idx++) {
//       const item = items[idx];

//       // if not enough space for one row + bottomSafety, finish current vertical segment, draw verticals,
//       // then create new page and redraw header and header separator.
//       if (y + rowHeight > doc.page.height - bottomSafety) {
//         // draw vertical separators for the segment we just filled on THIS page
//         if (segmentHeight > 0) {
//           drawVerticalsForSegment(segmentStartY, segmentHeight);
//         }

//         doc.addPage();
//         y = 36;

//         // redraw small page header area (logos not necessary) — keep consistent header rendering
//         try {
//           doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//         } catch (e) {}
//         try {
//           doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//         } catch (e) {}
//         // clinic name
//         doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//         y += 60;
//         // small dividing line
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 10;

//         // redraw table header on new page
//         // header background + border
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//         doc.text("Qty", colQtyX + 2, y + 3);
//         doc.text("Rate", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//         doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//         y += headerHeight;

//         // draw the single horizontal separator again under header
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset segment tracking for the new page
//         segmentStartY = y;
//         segmentHeight = 0;

//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row content (no horizontal box)
//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       // advance y and increment segmentHeight
//       y += rowHeight;
//       segmentHeight += rowHeight;
//     }

//     // After drawing items, we still want the table to have a minimum visual height.
//     // Draw empty rows (content only) to reach minTableHeight — but don't draw horizontal lines.
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       let remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);
//       for (let i = 0; i < emptyRows; i++) {
//         // handle page break while adding empty rows
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           // draw verticals for this page before page break
//           if (segmentHeight > 0) {
//             drawVerticalsForSegment(segmentStartY, segmentHeight);
//           }

//           doc.addPage();
//           y = 36;

//           // redraw condensed header area and table header on new page
//           try {
//             doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//           } catch (e) {}
//           try {
//             doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//           } catch (e) {}
//           doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//           doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//           y += 60;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 10;

//           // redraw table header
//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//           doc.text("Qty", colQtyX + 2, y + 3);
//           doc.text("Rate", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//           doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//           y += headerHeight;

//           // single horizontal separator under header
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           // reset segment tracking
//           segmentStartY = y;
//           segmentHeight = 0;

//           doc.font("Helvetica").fontSize(9);
//         }

//         // advance y for an empty visual row (no horizontal border)
//         y += rowHeight;
//         segmentHeight += rowHeight;
//       }
//     }

//     // finally, draw vertical separators for the last page's segment
//     if (segmentHeight > 0) {
//       drawVerticalsForSegment(segmentStartY, segmentHeight);
//     }

//     // small gap after table
//     y += 12;

//     // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH = 12;

//     doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Only show Total Due (compact)
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // move below totals box
//     y = boxY + lineH + 20;

//     // NET PAID + BALANCE (these remain)
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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



// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS (services take precedence)
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//       // sort by date ASC so earliest (first) payment is first in array
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     // primaryPayment = first (earliest) payment (explicit)
//     const primaryPayment = payments.length > 0 ? payments[0] : null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS (still used for math but not printed as a separate line)
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     // if bill.total is not present, compute from items (so adding items updates invoice)
//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // ensure invoice no is fixed once generated: if missing, generate and save back
//     const generatedInvoiceNo = `INV-${id}`;
//     const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
//     if (!bill.invoiceNo) {
//       // best-effort: persist invoice number so next time invoice remains same
//       try {
//         await billRef.update({ invoiceNo });
//       } catch (e) {
//         // non-fatal: continue even if update fails (e.g., permission)
//         console.warn("Failed to persist invoiceNo:", e);
//       }
//     }

//     const dateText = typeof formatDateDot === "function" ? formatDateDot(bill.date || "") : (bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 70; // amount
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200; // change this value to adjust fixed visual table height
//     const bottomSafety = 120; // reserved area to avoid overlapping footer

//     // We'll draw only one horizontal line after the header.
//     // We will NOT draw horizontal borders per row.
//     // Vertical separators will be drawn for the whole content block per page segment.

//     // helper to draw vertical separators for a vertical segment (on current page)
//     function drawVerticalsForSegment(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 3, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate", colRateX + 2, y + 3, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Amount", colSubX + 2, y + 3, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += headerHeight;

//     // draw a single horizontal separator line just below the header (one horizontal row after heading)
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // We will accumulate a single vertical segment per page.
//     let segmentStartY = y; // start of content area for current page
//     let segmentHeight = 0; // how tall content area grows on current page

//     doc.font("Helvetica").fontSize(9);

//     // iterate items and draw content (no horizontal borders). Manage page breaks and vertical segments.
//     for (let idx = 0; idx < items.length; idx++) {
//       const item = items[idx];

//       // if not enough space for one row + bottomSafety, finish current vertical segment, draw verticals,
//       // then create new page and redraw header and header separator.
//       if (y + rowHeight > doc.page.height - bottomSafety) {
//         // draw vertical separators for the segment we just filled on THIS page
//         if (segmentHeight > 0) {
//           drawVerticalsForSegment(segmentStartY, segmentHeight);
//         }

//         doc.addPage();
//         y = 36;

//         // redraw small page header area (logos not necessary) — keep consistent header rendering
//         try {
//           doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//         } catch (e) {}
//         try {
//           doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//         } catch (e) {}
//         // clinic name
//         doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//         y += 60;
//         // small dividing line
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 10;

//         // redraw table header on new page
//         // header background + border
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//         doc.text("Qty", colQtyX + 2, y + 3);
//         doc.text("Rate", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//         doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//         y += headerHeight;

//         // draw the single horizontal separator again under header
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset segment tracking for the new page
//         segmentStartY = y;
//         segmentHeight = 0;

//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row content (no horizontal box)
//       doc.text(String(idx + 1), colSrX + 2, y + 3);
//       doc.text(item.description || "", colServiceX + 2, y + 3, {
//         width: colServiceW - 4,
//       });
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 3
//       );
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 3, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 3, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       // advance y and increment segmentHeight
//       y += rowHeight;
//       segmentHeight += rowHeight;
//     }

//     // After drawing items, we still want the table to have a minimum visual height.
//     // Draw empty rows (content only) to reach minTableHeight — but don't draw horizontal lines.
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       let remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);
//       for (let i = 0; i < emptyRows; i++) {
//         // handle page break while adding empty rows
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           // draw verticals for this page before page break
//           if (segmentHeight > 0) {
//             drawVerticalsForSegment(segmentStartY, segmentHeight);
//           }

//           doc.addPage();
//           y = 36;

//           // redraw condensed header area and table header on new page
//           try {
//             doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//           } catch (e) {}
//           try {
//             doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//           } catch (e) {}
//           doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//           doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//           y += 60;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 10;

//           // redraw table header
//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//           doc.text("Qty", colQtyX + 2, y + 3);
//           doc.text("Rate", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//           doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//           y += headerHeight;

//           // single horizontal separator under header
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           // reset segment tracking
//           segmentStartY = y;
//           segmentHeight = 0;

//           doc.font("Helvetica").fontSize(9);
//         }

//         // advance y for an empty visual row (no horizontal border)
//         y += rowHeight;
//         segmentHeight += rowHeight;
//       }
//     }

//     // finally, draw vertical separators for the last page's segment
//     if (segmentHeight > 0) {
//       drawVerticalsForSegment(segmentStartY, segmentHeight);
//     }

//     // --- NEW: draw a single horizontal separator line AFTER the last content row (immediately above totals)
//     // Draw at current y (this is right after last row / filler). This is the "last wale row me horizontal line".
//     // Ensure it doesn't push totals — totals will be placed exactly after this line (margin 0).
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // Note: do NOT add extra gap here — totals should start immediately (margin 0).
//     // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     // place totals box with its top exactly at current y (no margin)
//     const boxY = y;
//     const lineH = 12;

//     // If there isn't enough vertical space to draw totals box at this position, shift to new page.
//     if (boxY + lineH + 40 > doc.page.height) {
//       doc.addPage();
//       y = 36;
//     }

//     doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Only show Total Due (compact)
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 2);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 2, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // set y just below totals box for the remaining content
//     y = boxY + lineH + 20;

//     // NET PAID + BALANCE (these remain)
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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


// app.get("/api/bills/:id/invoice-html-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     // 1) LEGACY ITEMS
//     const itemsSnap = await db
//       .collection("items")
//       .where("billId", "==", id)
//       .get();

//     const legacyItems = itemsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const qty = Number(d.qty || 0);
//       const rate = Number(d.rate || 0);
//       const amount = d.amount != null ? Number(d.amount) : qty * rate;

//       const description = d.description || d.item || d.details || "";

//       return {
//         id: doc.id,
//         qty,
//         rate,
//         amount,
//         description,
//       };
//     });

//     // 2) NEW SERVICES
//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;

//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);

//           return {
//             id: `svc-${idx + 1}`,
//             qty,
//             rate,
//             amount,
//             description: parts.join(" - "),
//           };
//         })
//       : [];

//     // 3) FINAL ITEMS (services take precedence)
//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // 4) PAYMENTS
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
//       // sort by date ASC so earliest (first) payment is first in array
//       .sort((a, b) => {
//         const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//         const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//         return da - dbb;
//       });

//     // primaryPayment = first (earliest) payment (explicit)
//     const primaryPayment = payments.length > 0 ? payments[0] : null;

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // 5) REFUNDS (still used for math but not printed as a separate line)
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", id)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       return Number(d.amount || 0);
//     });

//     const totalRefunded = refunds.reduce((sum, r) => sum + r, 0);

//     // 6) TOTALS
//     let total = Number(bill.total || 0);

//     // if bill.total is not present, compute from items (so adding items updates invoice)
//     if (!total && items.length > 0) {
//       total = items.reduce((sum, it) => sum + Number(it.amount || 0), 0);
//     }

//     const paidNet = totalPaidGross - totalRefunded;
//     const balance = total - paidNet;

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     // ensure invoice no is fixed once generated: if missing, generate and save back
//     const generatedInvoiceNo = `INV-${id}`;
//     const invoiceNo = bill.invoiceNo || generatedInvoiceNo;
//     if (!bill.invoiceNo) {
//       // best-effort: persist invoice number so next time invoice remains same
//       try {
//         await billRef.update({ invoiceNo });
//       } catch (e) {
//         // non-fatal: continue even if update fails (e.g., permission)
//         console.warn("Failed to persist invoiceNo:", e);
//       }
//     }

//     const dateText = typeof formatDateDot === "function" ? formatDateDot(bill.date || "") : (bill.date || "");
//     const patientName = bill.patientName || "";
//     const ageText =
//       bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const sexText = bill.sex ? String(bill.sex) : "";

//     const paymentMode = primaryPayment?.mode || "Cash";
//     const referenceNo = primaryPayment?.referenceNo || null;
//     const drawnOn = primaryPayment?.drawnOn || null;
//     const drawnAs = primaryPayment?.drawnAs || null;

//     const drawnOnText = drawnOn || "________________________";
//     const drawnAsText = drawnAs || "________________________";

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // 7) PDF START
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="invoice-${id}.pdf"`
//     );

//     const doc = new PDFDocument({
//       size: "A4",
//       margin: 36,
//     });

//     doc.pipe(res);

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 45, y, {
//         width: 45,
//         height: 45,
//       });
//     } catch (e) {}

//     // CLINIC HEADER (from profile)
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(16)
//       .text(clinicName || "", 0, y + 4, {
//         align: "center",
//         width: pageWidth,
//       });

//     doc
//       .font("Helvetica")
//       .fontSize(9)
//       .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
//       .text(
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 60;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();

//     y += 4;

//     // static doctor names replaced with profile values
//     doc.fontSize(9).font("Helvetica-Bold");
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // invoice title bar
//     doc.rect(36, y, usableWidth, 18).stroke();
//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("INVOICE CUM PAYMENT RECEIPT", 36, y + 4, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 26;

//     doc.font("Helvetica").fontSize(9);

//     // Invoice + Date row
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     // Mr/Mrs + Age row
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

//     // Address + Sex row (sex nayi line pe, address ke saath)
//     doc.text(`Address: ${bill.address || "________________________"}`, 36, y, {
//       width: usableWidth * 0.6,
//     });
//     if (sexText) {
//       doc.text(`Sex: ${sexText}`, pageWidth / 2, y, {
//         align: "right",
//         width: usableWidth / 2,
//       });
//     }

//     y += 20;

//     // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
//     const tableLeft = 36;
//     const colSrW = 22;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 70; // amount
//     const colServiceW =
//       usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200; // change this value to adjust fixed visual table height
//     const bottomSafety = 120; // reserved area to avoid overlapping footer

//     // We'll draw only one horizontal line after the header.
//     // We will NOT draw horizontal borders per row.
//     // Vertical separators will be drawn for the whole content block per page segment.

//     // helper to draw vertical separators for a vertical segment (on current page)
//     function drawVerticalsForSegment(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     // header text: add top padding by using y + 6
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 6);
//     doc.text("Description of Items / Services", colServiceX + 2, y + 6, {
//       width: colServiceW - 4,
//     });
//     doc.text("Qty", colQtyX + 2, y + 6);
//     doc.text("Rate", colRateX + 2, y + 6, {
//       width: colRateW - 4,
//       align: "right",
//     });
//     doc.text("Amount", colSubX + 2, y + 6, {
//       width: colSubW - 4,
//       align: "right",
//     });

//     y += headerHeight;

//     // draw a single horizontal separator line just below the header (one horizontal row after heading)
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // We will accumulate a single vertical segment per page.
//     let segmentStartY = y; // start of content area for current page
//     let segmentHeight = 0; // how tall content area grows on current page

//     doc.font("Helvetica").fontSize(9);

//     // iterate items and draw content (no horizontal borders). Manage page breaks and vertical segments.
//     for (let idx = 0; idx < items.length; idx++) {
//       const item = items[idx];

//       // if not enough space for one row + bottomSafety, finish current vertical segment, draw verticals,
//       // then create new page and redraw header and header separator.
//       if (y + rowHeight > doc.page.height - bottomSafety) {
//         // draw vertical separators for the segment we just filled on THIS page
//         if (segmentHeight > 0) {
//           drawVerticalsForSegment(segmentStartY, segmentHeight);
//         }

//         doc.addPage();
//         y = 36;

//         // redraw small page header area (logos not necessary) — keep consistent header rendering
//         try {
//           doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//         } catch (e) {}
//         try {
//           doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//         } catch (e) {}
//         // clinic name
//         doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//         y += 60;
//         // small dividing line
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 10;

//         // redraw table header on new page
//         // header background + border
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();

//         // header text on new page with top padding (y + 6)
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 6);
//         doc.text("Description of Items / Services", colServiceX + 2, y + 6, { width: colServiceW - 4 });
//         doc.text("Qty", colQtyX + 2, y + 6);
//         doc.text("Rate", colRateX + 2, y + 6, { width: colRateW - 4, align: "right" });
//         doc.text("Amount", colSubX + 2, y + 6, { width: colSubW - 4, align: "right" });
//         y += headerHeight;

//         // draw the single horizontal separator again under header
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset segment tracking for the new page
//         segmentStartY = y;
//         segmentHeight = 0;

//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row content (no horizontal box) with top padding for text (y + 6)
//       doc.text(String(idx + 1), colSrX + 2, y + 6);
//       doc.text(item.description || "", colServiceX + 2, y + 6, {
//         width: colServiceW - 4,
//       });
//       doc.text(
//         item.qty != null && item.qty !== "" ? String(item.qty) : "",
//         colQtyX + 2,
//         y + 6
//       );
//       doc.text(formatMoney(item.rate || 0), colRateX + 2, y + 6, {
//         width: colRateW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(item.amount || 0), colSubX + 2, y + 6, {
//         width: colSubW - 4,
//         align: "right",
//       });

//       // advance y and increment segmentHeight
//       y += rowHeight;
//       segmentHeight += rowHeight;
//     }

//     // After drawing items, we still want the table to have a minimum visual height.
//     // Draw empty rows (content only) to reach minTableHeight — but don't draw horizontal lines.
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       let remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);
//       for (let i = 0; i < emptyRows; i++) {
//         // handle page break while adding empty rows
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           // draw verticals for this page before page break
//           if (segmentHeight > 0) {
//             drawVerticalsForSegment(segmentStartY, segmentHeight);
//           }

//           doc.addPage();
//           y = 36;

//           // redraw condensed header area and table header on new page
//           try {
//             doc.image(logoLeftPath, leftX, y, { width: 45, height: 45 });
//           } catch (e) {}
//           try {
//             doc.image(logoRightPath, rightX - 45, y, { width: 45, height: 45 });
//           } catch (e) {}
//           doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
//           doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
//           y += 60;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 10;

//           // redraw table header
//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 6);
//           doc.text("Description of Items / Services", colServiceX + 2, y + 6, { width: colServiceW - 4 });
//           doc.text("Qty", colQtyX + 2, y + 6);
//           doc.text("Rate", colRateX + 2, y + 6, { width: colRateW - 4, align: "right" });
//           doc.text("Amount", colSubX + 2, y + 6, { width: colSubW - 4, align: "right" });
//           y += headerHeight;

//           // single horizontal separator under header
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           // reset segment tracking
//           segmentStartY = y;
//           segmentHeight = 0;

//           doc.font("Helvetica").fontSize(9);
//         }

//         // advance y for an empty visual row (no horizontal border)
//         y += rowHeight;
//         segmentHeight += rowHeight;
//       }
//     }

//     // finally, draw vertical separators for the last page's segment
//     if (segmentHeight > 0) {
//       drawVerticalsForSegment(segmentStartY, segmentHeight);
//     }

//     // --- NEW: draw a single horizontal separator line AFTER the last content row (immediately above totals)
//     // Draw at current y (this is right after last row / filler).
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // Note: do NOT add extra gap here — totals should start immediately (margin 0).
//     // 9) TOTALS BOX — removed visible "Refunded" line per request; still compute net paid & balance
//     const boxWidth = 180;
//     const boxX = pageWidth - 36 - boxWidth;
//     // place totals box with its top exactly at current y (no margin)
//     const boxY = y;
//     const lineH = 18; // make totals box slightly taller to give top padding inside

//     // If there isn't enough vertical space to draw totals box at this position, shift to new page.
//     if (boxY + lineH + 60 > doc.page.height) {
//       doc.addPage();
//       y = 36;
//     }

//     // draw totals box and border
//     doc.rect(boxX, boxY, boxWidth, lineH + 4).stroke();

//     doc.fontSize(9).font("Helvetica");

//     // Give top padding inside totals box by using boxY + 6
//     doc.font("Helvetica-Bold");
//     doc.text("Total Due", boxX + 6, boxY + 6);
//     doc.text(`Rs ${formatMoney(total)}`, boxX, boxY + 6, {
//       width: boxWidth - 6,
//       align: "right",
//     });

//     // set y just below totals box for the remaining content
//     y = boxY + lineH + 20;

//     // NET PAID + BALANCE (these remain)
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Amount Paid (net): Rs ${formatMoney(paidNet)}`, 36, y);
//     doc.text(`Balance: Rs ${formatMoney(balance)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 18;

//     // PAYMENT DETAILS BLOCK (shows first/earliest payment details)
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Payment Details:", 36, y);
//     y += 12;

//     doc.font("Helvetica").fontSize(8);
//     doc.text(`Mode: ${paymentMode || "________"}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`REF No.: ${referenceNo || "________"}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });
//     y += 12;

//     doc.text(`Drawn On: ${drawnOnText}`, 36, y, {
//       width: usableWidth / 2,
//     });
//     doc.text(`Drawn As: ${drawnAsText}`, pageWidth / 2, y, {
//       width: usableWidth / 2,
//       align: "right",
//     });

//     y += 20;

//     // FOOTER NOTES
//     doc
//       .fontSize(8)
//       .text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, {
//         width: usableWidth,
//       });

//     y = doc.y + 30;

//     // SIGNATURE LINES
//     const sigWidth = 160;
//     const sigY = y;

//     doc
//       .moveTo(36, sigY)
//       .lineTo(36 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, {
//       width: sigWidth,
//       align: "center",
//     });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc
//       .moveTo(rightSigX2, sigY)
//       .lineTo(rightSigX2 + sigWidth, sigY)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
//         width: sigWidth,
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
        const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
        const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
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
    const generatedInvoiceNo = `INV-${id}`;
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

    const dateText = typeof formatDateDot === "function" ? formatDateDot(bill.date || "") : (bill.date || "");
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
    const patientRepresentative = profileValue(profile, "patientRepresentative");
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
      .text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth })
      .text(
        (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
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
    doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
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

    // 8) SERVICES TABLE — reordered columns: Sr, Description, Qty, Rate, Amount
    const tableLeft = 36;
    const colSrW = 22;
    const colQtyW = 48;
    const colRateW = 70;
    const colSubW = 70; // amount
    const colServiceW =
      usableWidth - (colSrW + colQtyW + colRateW + colSubW);

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
    doc.text("Description of Items / Services", colServiceX + 2, headerTopY + 6, {
      width: colServiceW - 4,
    });
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
        doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
        doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
        y += 60;
        // small dividing line
        doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
        y += 10;

        // redraw table header on new page
        const headerTopY2 = y;
        // header background + border
        doc.save().rect(tableLeft, headerTopY2, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY2, usableWidth, headerHeight).stroke();

        // draw vertical separators for the header on the new page
        drawVerticalsForSegment(headerTopY2, headerHeight);

        // header text on new page with top padding (y + 6)
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Sr.", colSrX + 2, headerTopY2 + 6);
        doc.text("Description of Items / Services", colServiceX + 2, headerTopY2 + 6, { width: colServiceW - 4 });
        doc.text("Qty", colQtyX + 2, headerTopY2 + 6);
        doc.text("Rate", colRateX + 2, headerTopY2 + 6, { width: colRateW - 4, align: "right" });
        doc.text("Amount", colSubX + 2, headerTopY2 + 6, { width: colSubW - 4, align: "right" });
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
          doc.font("Helvetica-Bold").fontSize(16).text(clinicName || "", 0, y + 4, { align: "center", width: pageWidth });
          doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 24, { align: "center", width: pageWidth });
          y += 60;
          doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
          y += 10;

          // redraw table header
          const headerTopY3 = y;
          doc.save().rect(tableLeft, headerTopY3, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY3, usableWidth, headerHeight).stroke();

          // draw vertical separators for the header for this new page (filler case)
          drawVerticalsForSegment(headerTopY3, headerHeight);

          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Sr.", colSrX + 2, headerTopY3 + 6);
          doc.text("Description of Items / Services", colServiceX + 2, headerTopY3 + 6, { width: colServiceW - 4 });
          doc.text("Qty", colQtyX + 2, headerTopY3 + 6);
          doc.text("Rate", colRateX + 2, headerTopY3 + 6, { width: colRateW - 4, align: "right" });
          doc.text("Amount", colSubX + 2, headerTopY3 + 6, { width: colSubW - 4, align: "right" });
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
    doc.text("Total Due", boxX + 6, boxY + 6);
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
    doc
      .fontSize(8)
      .text(clinicRepresentative || "", rightSigX2, sigY + 4, {
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
    const patientRepresentative = profileValue(profile, "patientRepresentative");
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
        (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
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
    doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
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
    addDetail("Cheque No.:", chequeNumber);
    addDetail("Cheque Date:", formatDateDot(chequeDate));
    addDetail("Bank:", bankName);
    addDetail("Transfer Type:", transferType);
    addDetail("Transfer Date:", formatDateDot(transferDate));
    addDetail("UPI ID:", upiId);
    addDetail("UPI Name:", upiName);
    addDetail("UPI Date:", formatDateDot(upiDate));
    addDetail("Reference No.:", referenceNo);
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

    const sigY = y + 40;
    const sigWidth = 160;

    doc
      .moveTo(leftX, sigY)
      .lineTo(leftX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(patientRepresentative || "", leftX, sigY + 4, {
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
      .text(clinicRepresentative || "", rightSigX, sigY + 4, {
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
    const patientRepresentative = profileValue(profile, "patientRepresentative");
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

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
        (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
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
    doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
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

    let detailsY = doc.y + 4;
    doc.font("Helvetica");

    const addDetailR = (label, value) => {
      if (!value) return;
      doc.text(`${label} ${value}`, leftX, detailsY, { width: leftWidth });
      detailsY = doc.y + 3;
    };

    addDetailR("Amount Refunded: Rs", formatMoney(refund.amount));
    addDetailR("Refund Mode:", mode);
    addDetailR("Reference No.:", referenceNo);
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
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .text("Bill Summary", rightX + 6, by2);
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
    addRow2("Total Paid:", `Rs ${formatMoney(totalPaidGross)}`);
    addRow2("Refunded (incl. this):", `Rs ${formatMoney(refundedTillThis)}`);
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

    doc
      .moveTo(leftX, sigY)
      .lineTo(leftX + sigWidth, sigY)
      .dash(1, { space: 2 })
      .stroke()
      .undash();
    doc.fontSize(8).text(patientRepresentative || "", leftX, sigY + 4, {
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
      .text(clinicRepresentative || "", rightSigX, sigY + 4, {
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

// ---------- PDF: Bill Summary (A4 half page with chronological table) ----------
// app.get("/api/bills/:id/summary-pdf", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     const billTotal = Number(bill.total || 0);

//     // --- PAYMENTS ---
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       const paymentDateTime =
//         d.paymentDateTime ||
//         (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         paymentDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         receiptNo: d.receiptNo || null,
//       };
//     });

//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // --- REFUNDS ---
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const refundDateTime =
//         d.refundDateTime ||
//         (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         refundDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         refundNo: d.refundReceiptNo || null,
//       };
//     });

//     refunds.sort((a, b) => {
//       const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
//       const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalRefunded = refunds.reduce(
//       (sum, r) => sum + Number(r.amount || 0),
//       0
//     );

//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = billTotal - netPaid;

//     const paymentsCount = payments.length;
//     const refundsCount = refunds.length;

//     const patientName = bill.patientName || "";
//     const invoiceNo = bill.invoiceNo || billId;
//     const billDate = bill.date || "";
//     const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     function formatDateTime(dtString) {
//       if (!dtString) return "";
//       return formatDateTimeDot(dtString);
//     }

//     // --------- BUILD CHRONOLOGICAL TIMELINE ---------
//     const timeline = [];

//     const invoiceDateTime =
//       bill.createdAt || (bill.date ? `${bill.date}T00:00:00.000Z` : null);

//     timeline.push({
//       type: "INVOICE",
//       label: "Invoice Generated",
//       dateTime: invoiceDateTime,
//       mode: "-",
//       ref: invoiceNo,
//       debit: billTotal,
//       credit: 0,
//     });

//     payments.forEach((p) => {
//       timeline.push({
//         type: "PAYMENT",
//         label: p.receiptNo ? `Payment Receipt (${p.receiptNo})` : "Payment",
//         dateTime: p.paymentDateTime,
//         mode: p.mode || "",
//         ref: p.referenceNo || "",
//         debit: 0,
//         credit: p.amount,
//       });
//     });

//     refunds.forEach((r) => {
//       timeline.push({
//         type: "REFUND",
//         label: r.refundNo ? `Refund (${r.refundNo})` : "Refund",
//         dateTime: r.refundDateTime,
//         mode: r.mode || "",
//         ref: r.referenceNo || "",
//         debit: r.amount,
//         credit: 0,
//       });
//     });

//     timeline.sort((a, b) => {
//       const da = a.dateTime ? new Date(a.dateTime) : new Date(0);
//       const dbb = b.dateTime ? new Date(b.dateTime) : new Date(0);
//       return da - dbb;
//     });

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // ---------- PDF START ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="bill-summary-${billId}.pdf"`
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

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
//     } catch (e) {}

//     try {
//       doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
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
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();
//     y += 6;

//     // static doctor header
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 16;

//     // title bar
//     doc
//       .save()
//       .rect(36, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 16)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("BILL SUMMARY", 36, y + 3, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 24;

//     // invoice / patient line
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${formatDateDot(billDate)}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     doc.text(`Patient Name: ${patientName}`, 36, y, {
//       width: usableWidth,
//     });

//     y += 18;

//     // --------- CHRONOLOGICAL TABLE ---------
//     const tableLeft = 36;
//     const colDateW = 80;
//     const colPartW = 150;
//     const colModeW = 60;
//     const colRefW = 80;
//     const colDebitW = 50;
//     const colCreditW = 50;
//     const colBalW =
//       usableWidth -
//       (colDateW + colPartW + colModeW + colRefW + colDebitW + colCreditW);

//     const colDateX = tableLeft;
//     const colPartX = colDateX + colDateW;
//     const colModeX = colPartX + colPartW;
//     const colRefX = colModeX + colModeW;
//     const colDebitX = colRefX + colRefW;
//     const colCreditX = colDebitX + colDebitW;
//     const colBalX = colCreditX + colCreditW;

//     // header background
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, 16)
//       .stroke();

//     doc.font("Helvetica-Bold").fontSize(8);
//     doc.text("Date & Time", colDateX + 2, y + 3, {
//       width: colDateW - 4,
//     });
//     doc.text("Particulars", colPartX + 2, y + 3, {
//       width: colPartW - 4,
//     });
//     doc.text("Mode", colModeX + 2, y + 3, {
//       width: colModeW - 4,
//     });
//     doc.text("Reference", colRefX + 2, y + 3, {
//       width: colRefW - 4,
//     });
//     doc.text("Debit (Rs)", colDebitX + 2, y + 3, {
//       width: colDebitW - 4,
//       align: "right",
//     });
//     doc.text("Credit (Rs)", colCreditX + 2, y + 3, {
//       width: colCreditW - 4,
//       align: "right",
//     });
//     doc.text("Balance (Rs)", colBalX + 2, y + 3, {
//       width: colBalW - 4,
//       align: "right",
//     });

//     y += 16;
//     doc.font("Helvetica").fontSize(8);

//     let runningBalance = 0;

//     timeline.forEach((ev) => {
//       const rowHeight = 14;

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       if (ev.type === "INVOICE") {
//         runningBalance = ev.debit - ev.credit;
//       } else {
//         runningBalance += ev.debit;
//         runningBalance -= ev.credit;
//       }

//       doc.text(formatDateTime(ev.dateTime), colDateX + 2, y + 3, {
//         width: colDateW - 4,
//       });
//       doc.text(ev.label || "", colPartX + 2, y + 3, {
//         width: colPartW - 4,
//       });
//       doc.text(ev.mode || "", colModeX + 2, y + 3, {
//         width: colModeW - 4,
//       });
//       doc.text(ev.ref || "", colRefX + 2, y + 3, {
//         width: colRefW - 4,
//       });
//       doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 2, y + 3, {
//         width: colDebitW - 4,
//         align: "right",
//       });
//       doc.text(ev.credit ? formatMoney(ev.credit) : "", colCreditX + 2, y + 3, {
//         width: colCreditW - 4,
//         align: "right",
//       });
//       doc.text(formatMoney(runningBalance), colBalX + 2, y + 3, {
//         width: colBalW - 4,
//         align: "right",
//       });

//       y += rowHeight;
//     });

//     y += 18;

//     // --------- TOTALS BOX ---------
//     const boxWidth = 260;
//     const boxX = 36;
//     const boxY = y;
//     const lineH2 = 12;
//     const rows2 = 8;
//     const boxHeight = lineH2 * rows2 + 8;

//     doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

//     let by3 = boxY + 4;

//     doc.font("Helvetica").fontSize(9);

//     function row(label, value) {
//       doc.text(label, boxX + 6, by3);
//       doc.text(value, boxX + 6, by3, {
//         width: boxWidth - 12,
//         align: "right",
//       });
//       by3 += lineH2;
//     }

//     row("Bill Total", `Rs ${formatMoney(billTotal)}`);
//     row("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     row("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     row("Net Paid", `Rs ${formatMoney(netPaid)}`);
//     row("Balance", `Rs ${formatMoney(balance)}`);
//     row("Payments Count", String(paymentsCount));
//     row("Refunds Count", String(refundsCount));
//     row("Status", status);

//     const rightSigWidth = 160;
//     const sigY2 = boxY + boxHeight + 30;
//     const rightSigX = pageWidth - 36 - rightSigWidth;

//     doc
//       .moveTo(rightSigX, sigY2)
//       .lineTo(rightSigX + rightSigWidth, sigY2)
//       .dash(1, { space: 2 })
//       .stroke()
//       .undash();
//     doc
//       .fontSize(8)
//       .text(clinicRepresentative || "", rightSigX, sigY2 + 4, {
//         width: rightSigWidth,
//         align: "center",
//       });

//     doc.end();
//   } catch (err) {
//     console.error("summary-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate summary PDF" });
//     }
//   }
// });
// app.get("/api/bills/:id/summary-pdf", async (req, res) => {
//   const billId = req.params.id;
//   if (!billId) return res.status(400).json({ error: "Invalid bill id" });

//   try {
//     const billRef = db.collection("bills").doc(billId);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) {
//       return res.status(404).json({ error: "Bill not found" });
//     }
//     const bill = billSnap.data();

//     const billTotal = Number(bill.total || 0);

//     // --- PAYMENTS ---
//     const paysSnap = await db
//       .collection("payments")
//       .where("billId", "==", billId)
//       .get();

//     const payments = paysSnap.docs.map((doc) => {
//       const d = doc.data();
//       const paymentDateTime =
//         d.paymentDateTime ||
//         (d.paymentDate ? `${d.paymentDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         paymentDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         receiptNo: d.receiptNo || null,
//       };
//     });

//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalPaidGross = payments.reduce(
//       (sum, p) => sum + Number(p.amount || 0),
//       0
//     );

//     // --- REFUNDS ---
//     const refundsSnap = await db
//       .collection("refunds")
//       .where("billId", "==", billId)
//       .get();

//     const refunds = refundsSnap.docs.map((doc) => {
//       const d = doc.data();
//       const refundDateTime =
//         d.refundDateTime ||
//         (d.refundDate ? `${d.refundDate}T00:00:00.000Z` : null);
//       return {
//         id: doc.id,
//         amount: Number(d.amount || 0),
//         refundDateTime,
//         mode: d.mode || "",
//         referenceNo: d.referenceNo || null,
//         refundNo: d.refundReceiptNo || null,
//       };
//     });

//     refunds.sort((a, b) => {
//       const da = a.refundDateTime ? new Date(a.refundDateTime) : new Date(0);
//       const dbb = b.refundDateTime ? new Date(b.refundDateTime) : new Date(0);
//       return da - dbb;
//     });

//     const totalRefunded = refunds.reduce(
//       (sum, r) => sum + Number(r.amount || 0),
//       0
//     );

//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = billTotal - netPaid;

//     const paymentsCount = payments.length;
//     const refundsCount = refunds.length;

//     const patientName = bill.patientName || "";
//     const invoiceNo = bill.invoiceNo || billId;
//     const billDate = bill.date || "";
//     const status = bill.status || (balance <= 0 ? "PAID" : "PENDING");

//     function formatMoney(v) {
//       return Number(v || 0).toFixed(2);
//     }

//     function formatDateTime(dtString) {
//       if (!dtString) return "";
//       return typeof formatDateTimeDot === "function" ? formatDateTimeDot(dtString) : dtString;
//     }

//     // --------- BUILD CHRONOLOGICAL TIMELINE ---------
//     const timeline = [];

//     const invoiceDateTime =
//       bill.createdAt || (bill.date ? `${bill.date}T00:00:00.000Z` : null);

//     timeline.push({
//       type: "INVOICE",
//       label: "Invoice Generated",
//       dateTime: invoiceDateTime,
//       mode: "-",
//       ref: invoiceNo,
//       debit: billTotal,
//       credit: 0,
//     });

//     payments.forEach((p) => {
//       timeline.push({
//         type: "PAYMENT",
//         label: p.receiptNo ? `Payment Receipt (${p.receiptNo})` : "Payment",
//         dateTime: p.paymentDateTime,
//         mode: p.mode || "",
//         ref: p.referenceNo || "",
//         debit: 0,
//         credit: p.amount,
//       });
//     });

//     refunds.forEach((r) => {
//       timeline.push({
//         type: "REFUND",
//         label: r.refundNo ? `Refund (${r.refundNo})` : "Refund",
//         dateTime: r.refundDateTime,
//         mode: r.mode || "",
//         ref: r.referenceNo || "",
//         debit: r.amount,
//         credit: 0,
//       });
//     });

//     timeline.sort((a, b) => {
//       const da = a.dateTime ? new Date(a.dateTime) : new Date(0);
//       const dbb = b.dateTime ? new Date(b.dateTime) : new Date(0);
//       return da - dbb;
//     });

//     // ---------- FETCH CLINIC PROFILE FRESH FOR PDF ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // ---------- PDF START ----------
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader(
//       "Content-Disposition",
//       `inline; filename="bill-summary-${billId}.pdf"`
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

//     const leftX = 36;
//     const rightX = pageWidth - 36;

//     try {
//       doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
//     } catch (e) {}
//     try {
//       doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
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
//         (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
//         {
//           align: "center",
//           width: pageWidth,
//         }
//       );

//     y += 48;

//     doc
//       .moveTo(36, y)
//       .lineTo(pageWidth - 36, y)
//       .stroke();
//     y += 6;

//     // static doctor header
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text(doctor1Name || "", 36, y);
//     doc.text(doctor2Name || "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;
//     doc.font("Helvetica").fontSize(8);
//     doc.text(doctor1RegNo ? `Reg. No.: ${doctor1RegNo}` : "", 36, y);
//     doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 16;

//     // title bar (same style)
//     doc
//       .save()
//       .rect(36, y, usableWidth, 16)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(36, y, usableWidth, 16)
//       .stroke();

//     doc
//       .font("Helvetica-Bold")
//       .fontSize(10)
//       .text("BILL SUMMARY", 36, y + 3, {
//         align: "center",
//         width: usableWidth,
//       });

//     y += 24;

//     // invoice / patient line
//     doc.font("Helvetica").fontSize(9);
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${typeof formatDateDot === "function" ? formatDateDot(billDate) : billDate}`, pageWidth / 2, y, {
//       align: "right",
//       width: usableWidth / 2,
//     });

//     y += 12;

//     doc.text(`Patient Name: ${patientName}`, 36, y, {
//       width: usableWidth,
//     });

//     y += 18;

//     // --------- CHRONOLOGICAL TABLE (invoice-html-pdf style) ---------
//     const tableLeft = 36;
//     const colDateW = 70;
//     const colPartW = 170;
//     const colModeW = 50;
//     const colRefW = 80;
//     const colDebitW = 50;
//     const colCreditW = 50;
//     const colBalW =
//       usableWidth -
//       (colDateW + colPartW + colModeW + colRefW + colDebitW + colCreditW);

//     const colDateX = tableLeft;
//     const colPartX = colDateX + colDateW;
//     const colModeX = colPartX + colPartW;
//     const colRefX = colModeX + colModeW;
//     const colDebitX = colRefX + colRefW;
//     const colCreditX = colDebitX + colDebitW;
//     const colBalX = colCreditX + colCreditW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 18;
//     const rowHeight = 16;
//     const minTableHeight = 200;
//     const bottomSafety = 120;

//     // helper to draw vertical separators for a vertical segment (on current page)
//     function drawVerticalsForSegment(yTop, height) {
//       const xs = [colDateX, colPartX, colModeX, colRefX, colDebitX, colCreditX, colBalX, tableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     const headerTopY = y;
//     doc
//       .save()
//       .rect(tableLeft, headerTopY, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, headerTopY, usableWidth, headerHeight)
//       .stroke();

//     // draw vertical separators for header (same as body)
//     drawVerticalsForSegment(headerTopY, headerHeight);

//     doc.font("Helvetica-Bold").fontSize(8);
//     doc.text("Date & Time", colDateX + 4, headerTopY + 5, {
//       width: colDateW - 6,
//     });
//     doc.text("Particulars", colPartX + 4, headerTopY + 5, {
//       width: colPartW - 6,
//     });
//     doc.text("Mode", colModeX + 4, headerTopY + 5, {
//       width: colModeW - 6,
//     });
//     doc.text("Reference", colRefX + 4, headerTopY + 5, {
//       width: colRefW - 6,
//     });
//     doc.text("Debit (Rs)", colDebitX + 4, headerTopY + 5, {
//       width: colDebitW - 6,
//       align: "right",
//     });
//     doc.text("Credit (Rs)", colCreditX + 4, headerTopY + 5, {
//       width: colCreditW - 6,
//       align: "right",
//     });
//     doc.text("Balance (Rs)", colBalX + 4, headerTopY + 5, {
//       width: colBalW - 6,
//       align: "right",
//     });

//     y += headerHeight;

//     // draw a single horizontal separator line just below the header
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // accumulate vertical segment per page
//     let segmentStartY = y;
//     let segmentHeight = 0;

//     doc.font("Helvetica").fontSize(8);

//     let runningBalance = 0;

//     for (let i = 0; i < timeline.length; i++) {
//       const ev = timeline[i];

//       // page break check (space for one row + bottomSafety)
//       if (y + rowHeight > doc.page.height - bottomSafety) {
//         // draw verticals for filled segment on THIS page
//         if (segmentHeight > 0) {
//           drawVerticalsForSegment(segmentStartY, segmentHeight);
//         }

//         doc.addPage();
//         y = 36;

//         // redraw small page header area (logos optional)
//         try {
//           doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
//         } catch (e) {}
//         try {
//           doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
//         } catch (e) {}
//         // clinic name
//         doc.font("Helvetica-Bold").fontSize(13).text(clinicName || "", 0, y + 2, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 20, { align: "center", width: pageWidth });
//         y += 48;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 6;

//         // redraw header on new page
//         const headerTopY2 = y;
//         doc.save().rect(tableLeft, headerTopY2, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY2, usableWidth, headerHeight).stroke();
//         drawVerticalsForSegment(headerTopY2, headerHeight);

//         doc.font("Helvetica-Bold").fontSize(8);
//         doc.text("Date & Time", colDateX + 4, headerTopY2 + 5, { width: colDateW - 6 });
//         doc.text("Particulars", colPartX + 4, headerTopY2 + 5, { width: colPartW - 6 });
//         doc.text("Mode", colModeX + 4, headerTopY2 + 5, { width: colModeW - 6 });
//         doc.text("Reference", colRefX + 4, headerTopY2 + 5, { width: colRefW - 6 });
//         doc.text("Debit (Rs)", colDebitX + 4, headerTopY2 + 5, { width: colDebitW - 6, align: "right" });
//         doc.text("Credit (Rs)", colCreditX + 4, headerTopY2 + 5, { width: colCreditW - 6, align: "right" });
//         doc.text("Balance (Rs)", colBalX + 4, headerTopY2 + 5, { width: colBalW - 6, align: "right" });

//         y += headerHeight;
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset tracking
//         segmentStartY = y;
//         segmentHeight = 0;

//         doc.font("Helvetica").fontSize(8);
//       }

//       // compute running balance
//       if (ev.type === "INVOICE") {
//         runningBalance = ev.debit - ev.credit;
//       } else {
//         runningBalance += ev.debit;
//         runningBalance -= ev.credit;
//       }

//       // row content (no horizontal borders, we used vertical separators instead)
//       doc.text(formatDateTime(ev.dateTime), colDateX + 4, y + 3, { width: colDateW - 6 });
//       doc.text(ev.label || "", colPartX + 4, y + 3, { width: colPartW - 6 });
//       doc.text(ev.mode || "", colModeX + 4, y + 3, { width: colModeW - 6 });
//       doc.text(ev.ref || "", colRefX + 4, y + 3, { width: colRefW - 6 });
//       doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 4, y + 3, { width: colDebitW - 6, align: "right" });
//       doc.text(ev.credit ? formatMoney(ev.credit) : "", colCreditX + 4, y + 3, { width: colCreditW - 6, align: "right" });
//       doc.text(formatMoney(runningBalance), colBalX + 4, y + 3, { width: colBalW - 6, align: "right" });

//       // advance y and segmentHeight
//       y += rowHeight;
//       segmentHeight += rowHeight;
//     }

//     // After drawing timeline rows, ensure table has minimum visual height (fill empty rows)
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       let remainingHeight = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remainingHeight / rowHeight);
//       for (let i = 0; i < emptyRows; i++) {
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           // draw verticals for this page before page break
//           if (segmentHeight > 0) {
//             drawVerticalsForSegment(segmentStartY, segmentHeight);
//           }

//           doc.addPage();
//           y = 36;

//           // redraw condensed header area and table header on new page
//           try {
//             doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
//           } catch (e) {}
//           try {
//             doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
//           } catch (e) {}
//           doc.font("Helvetica-Bold").fontSize(13).text(clinicName || "", 0, y + 2, { align: "center", width: pageWidth });
//           doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 20, { align: "center", width: pageWidth });
//           y += 48;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 6;

//           // redraw header
//           const headerTopY3 = y;
//           doc.save().rect(tableLeft, headerTopY3, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY3, usableWidth, headerHeight).stroke();
//           drawVerticalsForSegment(headerTopY3, headerHeight);

//           doc.font("Helvetica-Bold").fontSize(8);
//           doc.text("Date & Time", colDateX + 4, headerTopY3 + 5, { width: colDateW - 6 });
//           doc.text("Particulars", colPartX + 4, headerTopY3 + 5, { width: colPartW - 6 });
//           doc.text("Mode", colModeX + 4, headerTopY3 + 5, { width: colModeW - 6 });
//           doc.text("Reference", colRefX + 4, headerTopY3 + 5, { width: colRefW - 6 });
//           doc.text("Debit (Rs)", colDebitX + 4, headerTopY3 + 5, { width: colDebitW - 6, align: "right" });
//           doc.text("Credit (Rs)", colCreditX + 4, headerTopY3 + 5, { width: colCreditW - 6, align: "right" });
//           doc.text("Balance (Rs)", colBalX + 4, headerTopY3 + 5, { width: colBalW - 6, align: "right" });

//           y += headerHeight;
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           // reset tracking
//           segmentStartY = y;
//           segmentHeight = 0;

//           doc.font("Helvetica").fontSize(8);
//         }

//         // add empty visual row (no horizontal border)
//         y += rowHeight;
//         segmentHeight += rowHeight;
//       }
//     }

//     // finally, draw vertical separators for the last page's segment
//     if (segmentHeight > 0) {
//       drawVerticalsForSegment(segmentStartY, segmentHeight);
//     }

//     // draw a single horizontal separator line after the last content row (immediately above totals)
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // --------- TOTALS BOX (placed on right like invoice) ---------
//     const boxWidth = 220;
//     const boxX = pageWidth - 36 - boxWidth;
//     const boxY = y;
//     const lineH2 = 16; // row height in box
//     const rows2 = 6;
//     const boxHeight = lineH2 * rows2 + 8;

//     // If not enough space, move to new page
//     if (boxY + boxHeight + 60 > doc.page.height) {
//       doc.addPage();
//       y = 36;
//     }

//     doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

//     let by3 = boxY + 6;
//     doc.font("Helvetica").fontSize(9);
//     function boxRow(label, value) {
//       doc.font("Helvetica").fontSize(9).text(label, boxX + 8, by3);
//       doc.text(value, boxX + 8, by3, { width: boxWidth - 16, align: "right" });
//       by3 += lineH2;
//     }

//     boxRow("Bill Total", `Rs ${formatMoney(billTotal)}`);
//     boxRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     boxRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     boxRow("Net Paid", `Rs ${formatMoney(netPaid)}`);
//     boxRow("Balance", `Rs ${formatMoney(balance)}`);
//     boxRow("Status", status);

//     // move y below totals box for signature / footer
//     y = boxY + boxHeight + 20;

//     // FOOTER NOTES (left)
//     doc.fontSize(8).text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, { width: usableWidth });

//     y = y + 30;

//     // SIGNATURE LINES (patient left, clinic right) - similar to invoice
//     const sigWidth = 160;
//     const sigY = y;

//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });

//     const rightSigX2 = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX2, sigY).lineTo(rightSigX2 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX2, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("summary-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate summary PDF" });
//     }
//   }
// });

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
      return typeof formatDateTimeDot === "function" ? formatDateTimeDot(dtString) : dtString;
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
    const patientRepresentative = profileValue(profile, "patientRepresentative");
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
        (clinicPAN || "") + (clinicPAN || clinicRegNo ? "   |   " : "") + (clinicRegNo || ""),
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
    doc.text(doctor2RegNo ? `Reg. No.: ${doctor2RegNo}` : "", pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 16;

    // title bar (same style)
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
    doc.text(`Date: ${typeof formatDateDot === "function" ? formatDateDot(billDate) : billDate}`, pageWidth / 2, y, {
      align: "right",
      width: usableWidth / 2,
    });

    y += 12;

    doc.text(`Patient Name: ${patientName}`, 36, y, {
      width: usableWidth,
    });

    y += 18;

    // --------- CHRONOLOGICAL TABLE (invoice-html-pdf style) ---------
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

    // layout constants (we'll use dynamic row heights)
    const headerHeight = 18;
    const minRowHeight = 14; // minimum per-row height
    const minTableHeight = 200;
    const bottomSafety = 120;

    // helper to draw vertical separators for a vertical segment (on current page)
    function drawVerticalsForSegment(yTop, height) {
      const xs = [colDateX, colPartX, colModeX, colRefX, colDebitX, colCreditX, colBalX, tableRightX];
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

    // draw vertical separators for header (same as body)
    drawVerticalsForSegment(headerTopY, headerHeight);

    doc.font("Helvetica-Bold").fontSize(8);
    doc.text("Date & Time", colDateX + 4, headerTopY + 5, {
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

    // draw a single horizontal separator line just below the header
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // accumulate vertical segment per page
    let segmentStartY = y;
    let segmentHeight = 0;

    doc.font("Helvetica").fontSize(8);

    let runningBalance = 0;

    // iterate timeline and render rows with dynamic heights
    for (let i = 0; i < timeline.length; i++) {
      const ev = timeline[i];

      // Prepare cell strings
      const dateStr = formatDateTime(ev.dateTime);
      const partStr = ev.label || "";
      const modeStr = ev.mode || "";
      const refStr = ev.ref || "";
      const debitStr = ev.debit ? formatMoney(ev.debit) : "";
      const creditStr = ev.credit ? formatMoney(ev.credit) : "";

      // compute heights for each cell given available column width and current font settings
      // use a small padding (top + bottom) for aesthetics
      const padding = 6; // total vertical padding (we add on top when placing text)
      const dateH = doc.heightOfString(String(dateStr), { width: colDateW - 8 });
      const partH = doc.heightOfString(String(partStr), { width: colPartW - 8 });
      const modeH = doc.heightOfString(String(modeStr), { width: colModeW - 8 });
      const refH = doc.heightOfString(String(refStr), { width: colRefW - 8 });
      const debitH = doc.heightOfString(String(debitStr), { width: colDebitW - 8 });
      const creditH = doc.heightOfString(String(creditStr), { width: colCreditW - 8 });
      const balH = doc.heightOfString(String(formatMoney(
        ev.type === "INVOICE"
          ? (ev.debit - ev.credit)
          : (runningBalance + ev.debit - ev.credit)
      )), { width: colBalW - 8 });

      // Determine the row height as the max of cell heights + padding; enforce minimum
      const contentMaxH = Math.max(dateH, partH, modeH, refH, debitH, creditH, balH);
      let rowH = Math.max(minRowHeight, contentMaxH + padding);

      // Before rendering, check for page break: need space for this row + bottomSafety
      if (y + rowH > doc.page.height - bottomSafety) {
        // draw verticals for the segment we just filled on THIS page
        if (segmentHeight > 0) {
          drawVerticalsForSegment(segmentStartY, segmentHeight);
        }

        doc.addPage();
        y = 36;

        // redraw small page header area (logos optional)
        try {
          doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
        } catch (e) {}
        try {
          doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
        } catch (e) {}
        // clinic name
        doc.font("Helvetica-Bold").fontSize(13).text(clinicName || "", 0, y + 2, { align: "center", width: pageWidth });
        doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 20, { align: "center", width: pageWidth });
        y += 48;
        doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
        y += 6;

        // redraw header on new page
        const headerTopY2 = y;
        doc.save().rect(tableLeft, headerTopY2, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY2, usableWidth, headerHeight).stroke();
        drawVerticalsForSegment(headerTopY2, headerHeight);

        doc.font("Helvetica-Bold").fontSize(8);
        doc.text("Date & Time", colDateX + 4, headerTopY2 + 5, { width: colDateW - 6 });
        doc.text("Particulars", colPartX + 4, headerTopY2 + 5, { width: colPartW - 6 });
        doc.text("Mode", colModeX + 4, headerTopY2 + 5, { width: colModeW - 6 });
        doc.text("Reference", colRefX + 4, headerTopY2 + 5, { width: colRefW - 6 });
        doc.text("Debit (Rs)", colDebitX + 4, headerTopY2 + 5, { width: colDebitW - 6, align: "right" });
        doc.text("Credit (Rs)", colCreditX + 4, headerTopY2 + 5, { width: colCreditW - 6, align: "right" });
        doc.text("Balance (Rs)", colBalX + 4, headerTopY2 + 5, { width: colBalW - 6, align: "right" });

        y += headerHeight;
        doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

        // reset segment tracking for the new page
        segmentStartY = y;
        segmentHeight = 0;

        doc.font("Helvetica").fontSize(8);
      }

      // compute running balance BEFORE drawing balance cell so height calc aligns with displayed balance
      if (ev.type === "INVOICE") {
        runningBalance = ev.debit - ev.credit;
      } else {
        runningBalance += ev.debit;
        runningBalance -= ev.credit;
      }

      // Draw the content at y with vertical padding of 3 (top)
      const cellTop = y + 3;
      doc.text(dateStr, colDateX + 4, cellTop, { width: colDateW - 8 });
      doc.text(partStr, colPartX + 4, cellTop, { width: colPartW - 8 });
      doc.text(modeStr, colModeX + 4, cellTop, { width: colModeW - 8 });
      doc.text(refStr, colRefX + 4, cellTop, { width: colRefW - 8 });
      doc.text(ev.debit ? formatMoney(ev.debit) : "", colDebitX + 4, cellTop, { width: colDebitW - 8, align: "right" });
      doc.text(ev.credit ? formatMoney(ev.credit) : "", colCreditX + 4, cellTop, { width: colCreditW - 8, align: "right" });
      doc.text(formatMoney(runningBalance), colBalX + 4, cellTop, { width: colBalW - 8, align: "right" });

      // Advance y by rowH and add to segmentHeight
      y += rowH;
      segmentHeight += rowH;
    }

    // After drawing timeline rows, ensure table has minimum visual height (fill empty rows)
    const currentTableHeight = y - tableStartY;
    if (currentTableHeight < minTableHeight) {
      let remainingHeight = minTableHeight - currentTableHeight;
      // for filler rows we'll use minRowHeight
      const emptyRows = Math.ceil(remainingHeight / minRowHeight);
      for (let i = 0; i < emptyRows; i++) {
        // page break handling for filler rows
        if (y + minRowHeight > doc.page.height - bottomSafety) {
          if (segmentHeight > 0) {
            drawVerticalsForSegment(segmentStartY, segmentHeight);
          }
          doc.addPage();
          y = 36;

          // redraw condensed header area and table header on new page
          try {
            doc.image(logoLeftPath, leftX, y, { width: 32, height: 32 });
          } catch (e) {}
          try {
            doc.image(logoRightPath, rightX - 32, y, { width: 32, height: 32 });
          } catch (e) {}
          doc.font("Helvetica-Bold").fontSize(13).text(clinicName || "", 0, y + 2, { align: "center", width: pageWidth });
          doc.font("Helvetica").fontSize(9).text(clinicAddress || "", 0, y + 20, { align: "center", width: pageWidth });
          y += 48;
          doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
          y += 6;

          const headerTopY3 = y;
          doc.save().rect(tableLeft, headerTopY3, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, headerTopY3, usableWidth, headerHeight).stroke();
          drawVerticalsForSegment(headerTopY3, headerHeight);

          doc.font("Helvetica-Bold").fontSize(8);
          doc.text("Date & Time", colDateX + 4, headerTopY3 + 5, { width: colDateW - 6 });
          doc.text("Particulars", colPartX + 4, headerTopY3 + 5, { width: colPartW - 6 });
          doc.text("Mode", colModeX + 4, headerTopY3 + 5, { width: colModeW - 6 });
          doc.text("Reference", colRefX + 4, headerTopY3 + 5, { width: colRefW - 6 });
          doc.text("Debit (Rs)", colDebitX + 4, headerTopY3 + 5, { width: colDebitW - 6, align: "right" });
          doc.text("Credit (Rs)", colCreditX + 4, headerTopY3 + 5, { width: colCreditW - 6, align: "right" });
          doc.text("Balance (Rs)", colBalX + 4, headerTopY3 + 5, { width: colBalW - 6, align: "right" });

          y += headerHeight;
          doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

          segmentStartY = y;
          segmentHeight = 0;
          doc.font("Helvetica").fontSize(8);
        }

        // add an empty visual filler row
        y += minRowHeight;
        segmentHeight += minRowHeight;
      }
    }

    // finally, draw vertical separators for the last page's segment
    if (segmentHeight > 0) {
      drawVerticalsForSegment(segmentStartY, segmentHeight);
    }

    // draw a single horizontal separator line after the last content row (immediately above totals)
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // --------- TOTALS BOX (placed on right like invoice) ---------
    const boxWidth = 220;
    const boxX = pageWidth - 36 - boxWidth;
    const boxY = y;
    const lineH2 = 16; // row height in box
    const rows2 = 6;
    const boxHeight = lineH2 * rows2 + 8;

    // If not enough space, move to new page
    if (boxY + boxHeight + 60 > doc.page.height) {
      doc.addPage();
      y = 36;
    }

    doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();

    let by3 = boxY + 6;
    doc.font("Helvetica").fontSize(9);
    function boxRow(label, value) {
      doc.font("Helvetica").fontSize(9).text(label, boxX + 8, by3);
      doc.text(value, boxX + 8, by3, { width: boxWidth - 16, align: "right" });
      by3 += lineH2;
    }

    boxRow("Bill Total", `Rs ${formatMoney(billTotal)}`);
    boxRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
    boxRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
    boxRow("Net Paid", `Rs ${formatMoney(netPaid)}`);
    boxRow("Balance", `Rs ${formatMoney(balance)}`);
    boxRow("Status", status);

    // move y below totals box for signature / footer
    y = boxY + boxHeight + 20;

    // FOOTER NOTES (left)
    doc.fontSize(8).text("* Dispute if any Subject to Jamshedpur Jurisdiction", 36, y, { width: usableWidth });

    y = y + 30;

    // SIGNATURE LINES (patient left, clinic right) - similar to invoice
    const sigWidth = 160;
    const sigY = y;

    doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
    doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });

    const rightSigX2 = pageWidth - 36 - sigWidth;
    doc.moveTo(rightSigX2, sigY).lineTo(rightSigX2 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX2, sigY + 4, { width: sigWidth, align: "center" });

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
    const { patientName, sex, address, age, date, remarks, services } =
      req.body;

    const jsDate =
      date || oldBill.date || new Date().toISOString().slice(0, 10);

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
      date: formatDateDot(jsDate),
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

// app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   function formatMoney(v) { return Number(v || 0).toFixed(2); }

//   function formatDateOnly(dtString) {
//     return formatDateDot(dtString);
//   }

//   try {
//     // load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
//     const bill = billSnap.data();

//     // fetch items (legacy/new combined)
//     const itemsSnap = await db.collection("items").where("billId", "==", id).get();
//     const legacyItems = itemsSnap.docs.map((d) => {
//       const dd = d.data();
//       return {
//         id: d.id,
//         description: dd.description || dd.item || dd.details || "",
//         qty: Number(dd.qty || 0),
//         rate: Number(dd.rate || 0),
//         amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
//       };
//     });

//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;
//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);
//           return {
//             id: `svc-${idx + 1}`,
//             description: parts.join(" - "),
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // payments & refunds
//     const paysSnap = await db.collection("payments").where("billId", "==", id).get();
//     const payments = paysSnap.docs.map((d) => {
//       const pd = d.data();
//       return {
//         id: d.id,
//         paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
//         paymentDate: pd.paymentDate || null,
//         paymentTime: pd.paymentTime || null,
//         amount: Number(pd.amount || 0),
//         mode: pd.mode || "",
//         referenceNo: pd.referenceNo || "",
//         chequeDate: pd.chequeDate || null,
//         chequeNumber: pd.chequeNumber || null,
//         bankName: pd.bankName || null,
//         transferType: pd.transferType || null,
//         transferDate: pd.transferDate || null,
//         upiName: pd.upiName || null,
//         upiId: pd.upiId || null,
//         upiDate: pd.upiDate || null,
//         drawnOn: pd.drawnOn || null,
//         drawnAs: pd.drawnAs || null,
//         receiptNo: pd.receiptNo || d.id,
//       };
//     });

//     const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
//     const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

//     // sort payments chronologically
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     // totals
//     const total = Number(bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0));
//     const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
//     const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = total - netPaid;

//     // Only allow full-payment PDF if balance is zero (or less)
//     if (balance > 0) {
//       return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
//     }

//     // ---------- FETCH CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");


//     // --- PDF Setup ---
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

//     const doc = new PDFDocument({ size: "A4", margin: 36 });
//     doc.pipe(res);

//     // register local font if available (optional)
//     try {
//       const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
//       if (fs && fs.existsSync(workSansPath)) {
//         doc.registerFont("WorkSans", workSansPath);
//         doc.font("WorkSans");
//       } else {
//         doc.font("Helvetica");
//       }
//     } catch (e) {
//       doc.font("Helvetica");
//     }

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     // Header logos (try-catch because resources may not exist)
//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
//     try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//     try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

//     // Clinic header (from profile)
//     doc.fontSize(14).font("Helvetica-Bold").text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//     doc.fontSize(9).font("Helvetica").text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//     doc.text(`PAN : ${clinicPAN}   |   Reg. No: ${clinicRegNo}`, { align: "center", width: pageWidth });

//     y += 56;
//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 8;

//     // Invoice Title
//     doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//     doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//     y += 28;

//     // Invoice details line (Invoice No + Date) - date only DD.MM.YYYY
//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateOnly(bill.date || "");
//     doc.fontSize(9).font("Helvetica");
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     // Patient info (address & sex swapped)
//     const patientName = bill.patientName || "";
//     const sexText = bill.sex ? String(bill.sex) : "";
//     const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const addressText = bill.address || "";

//     doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
//     doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
//     doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 20;

//     // ---------- SERVICES / ITEMS TABLE ----------
//     const tableLeft = 36;
//     const colSrW = 24;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colAdjW = 60;
//     const colSubW = 80;
//     const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

//     let xSr = tableLeft;
//     let xQty = xSr + colSrW;
//     let xService = xQty + colQtyW;
//     let xRate = xService + colServiceW;
//     let xAdj = xRate + colRateW;
//     let xSub = xAdj + colAdjW;

//     // header
//     doc.save().rect(tableLeft, y, usableWidth, 16).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, 16).stroke();
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", xSr + 2, y + 3);
//     doc.text("Qty", xQty + 2, y + 3);
//     doc.text("Procedure", xService + 2, y + 3, { width: colServiceW - 4 });
//     doc.text("Rate / Price", xRate + 2, y + 3, { width: colRateW - 4, align: "right" });
//     doc.text("Amount", xSub + 2, y + 3, { width: colSubW - 4, align: "right" });
//     y += 16;

//     // rows (dynamic height)
//     doc.font("Helvetica").fontSize(9);
//     const svcMinRowH = 14;
//     for (let i = 0; i < items.length; i++) {
//       const it = items[i];
//       const rowPadding = 6;

//       const descH = doc.heightOfString(it.description || "", { width: colServiceW - 4 });
//       const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 4 });
//       const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 4 });
//       const amountH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 4 });

//       const rowTextMaxH = Math.max(descH, qtyH, rateH, amountH);
//       const rowHeight = Math.max(svcMinRowH, rowTextMaxH + rowPadding);

//       doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();

//       const descY = y + (rowHeight - descH) / 2;
//       const qtyY = y + (rowHeight - qtyH) / 2;
//       const rateY = y + (rowHeight - rateH) / 2;
//       const amountY = y + (rowHeight - amountH) / 2;

//       doc.text(String(i + 1), xSr + 2, y + 3);
//       doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), xQty + 2, qtyY, { width: colQtyW - 4, align: "left" });
//       doc.text(it.description || "", xService + 2, descY, { width: colServiceW - 4 });
//       doc.text(formatMoney(it.rate || 0), xRate + 2, rateY, { width: colRateW - 4, align: "right" });
//       doc.text(formatMoney(it.amount || 0), xSub + 2, amountY, { width: colSubW - 4, align: "right" });

//       y += rowHeight;

//       if (y > doc.page.height - 160) {
//         doc.addPage();
//         y = 36;
//       }
//     }

//     y += 12;

//     // ---------- PAYMENT DETAILS (chronological) ----------
//     const pTableLeft = 36;
//     const pColDateW = 100; // reduced width since time removed
//     const pColRecW = 140;
//     const pColModeW = 90;
//     const pColBankW = 110;
//     const pColRefW = 110;
//     const pColAmtW = usableWidth - (pColDateW + pColRecW + pColModeW + pColBankW + pColRefW);

//     const pColDateX = pTableLeft;
//     const pColRecX = pColDateX + pColDateW;
//     const pColModeX = pColRecX + pColRecW;
//     const pColBankX = pColModeX + pColModeW;
//     const pColRefX = pColBankX + pColBankW;
//     const pColAmtX = pColRefX + pColRefW;

//     // header
//     doc.save().rect(pTableLeft, y, usableWidth, 18).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, 18).stroke();
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//     doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//     doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//     doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//     doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//     doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
//     y += 18;

//     const pMinRowH = 16;
//     for (const p of payments) {
//       const dateTextP = formatDateOnly(p.paymentDateTime || p.paymentDate || `${p.paymentDate || ""}`);
//       const receiptText = p.receiptNo || p.id || "";
//       let modeText = p.mode || "-";
//       if ((modeText === "BankTransfer" || modeText.toLowerCase().includes("bank")) && p.transferType) {
//         modeText = `Bank (${p.transferType})`;
//       }
//       const bankText = p.bankName || "-";
//       const refText = p.referenceNo || "-";
//       const amtText = formatMoney(p.amount || 0);

//       doc.font("Helvetica").fontSize(9);
//       const dH = doc.heightOfString(dateTextP, { width: pColDateW - 8 });
//       const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
//       const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
//       const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
//       const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
//       const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

//       const maxH = Math.max(dH, rH, mH, bH, refH, aH);
//       const rowH = Math.max(pMinRowH, maxH + 8);

//       doc.rect(pTableLeft, y, usableWidth, rowH).stroke();

//       const dateY = y + (rowH - dH) / 2;
//       const recY = y + (rowH - rH) / 2;
//       const modeY = y + (rowH - mH) / 2;
//       const bankY = y + (rowH - bH) / 2;
//       const refY = y + (rowH - refH) / 2;
//       const amtY = y + (rowH - aH) / 2;

//       doc.text(dateTextP, pColDateX + 4, dateY, { width: pColDateW - 8 });
//       doc.text(receiptText, pColRecX + 4, recY, { width: pColRecW - 8 });
//       doc.text(modeText, pColModeX + 4, modeY, { width: pColModeW - 8 });
//       doc.text(bankText, pColBankX + 4, bankY, { width: pColBankW - 8 });
//       doc.text(refText, pColRefX + 4, refY, { width: pColRefW - 8 });
//       doc.text(amtText, pColAmtX + 4, amtY, { width: pColAmtW - 8, align: "right" });

//       y += rowH;

//       if (y > doc.page.height - 140) {
//         doc.addPage();
//         y = 36;
//         // redraw payments header on new page
//         doc.save().rect(pTableLeft, y, usableWidth, 18).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, 18).stroke();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//         doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//         doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//         doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//         doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//         doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
//         y += 18;
//       }
//     }

//     y += 12;

//     // ---------- TOTALS BOX (after payment table) ----------
//     const boxWidth2 = 260;
//     const boxX2 = 36;
//     const boxY2 = y;
//     const lineH = 12;
//     const rowsCount = 5;
//     const boxHeight2 = rowsCount * lineH + 8;

//     doc.rect(boxX2, boxY2, boxWidth2, boxHeight2).stroke();
//     let by = boxY2 + 6;
//     doc.font("Helvetica").fontSize(9);

//     const addRow = (label, value) => {
//       doc.text(label, boxX2 + 6, by);
//       doc.text(value, boxX2 + 6, by, { width: boxWidth2 - 12, align: "right" });
//       by += lineH;
//     };

//     addRow("Total", `Rs ${formatMoney(total)}`);
//     addRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     addRow("Net Paid (after refunds)", `Rs ${formatMoney(netPaid)}`);
//     addRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     addRow("Balance", `Rs ${formatMoney(balance)}`);

//     y = boxY2 + boxHeight2 + 20;

//     // footer note + signatures
//     doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
//     const sigY = y + 28;
//     const sigWidth = 160;
//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative, 36, sigY + 4, { width: sigWidth, align: "center" });
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative, rightSigX, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("full-payment-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate full payment PDF" });
//     } else {
//       try { res.end(); } catch (e) {}
//     }
//   }
// });

// app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   function formatMoney(v) { return Number(v || 0).toFixed(2); }

//   function formatDateOnly(dtString) {
//     return typeof formatDateDot === "function" ? formatDateDot(dtString) : dtString;
//   }

//   try {
//     // load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
//     const bill = billSnap.data();

//     // fetch items (legacy/new combined)
//     const itemsSnap = await db.collection("items").where("billId", "==", id).get();
//     const legacyItems = itemsSnap.docs.map((d) => {
//       const dd = d.data();
//       return {
//         id: d.id,
//         description: dd.description || dd.item || dd.details || "",
//         qty: Number(dd.qty || 0),
//         rate: Number(dd.rate || 0),
//         amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
//       };
//     });

//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;
//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);
//           return {
//             id: `svc-${idx + 1}`,
//             description: parts.join(" - "),
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // payments & refunds
//     const paysSnap = await db.collection("payments").where("billId", "==", id).get();
//     const payments = paysSnap.docs.map((d) => {
//       const pd = d.data();
//       return {
//         id: d.id,
//         paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
//         paymentDate: pd.paymentDate || null,
//         paymentTime: pd.paymentTime || null,
//         amount: Number(pd.amount || 0),
//         mode: pd.mode || "",
//         referenceNo: pd.referenceNo || "",
//         chequeDate: pd.chequeDate || null,
//         chequeNumber: pd.chequeNumber || null,
//         bankName: pd.bankName || null,
//         transferType: pd.transferType || null,
//         transferDate: pd.transferDate || null,
//         upiName: pd.upiName || null,
//         upiId: pd.upiId || null,
//         upiDate: pd.upiDate || null,
//         drawnOn: pd.drawnOn || null,
//         drawnAs: pd.drawnAs || null,
//         receiptNo: pd.receiptNo || d.id,
//       };
//     });

//     const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
//     const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

//     // sort payments chronologically
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     // totals
//     const total = Number(bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0));
//     const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
//     const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = total - netPaid;

//     // Only allow full-payment PDF if balance is zero (or less)
//     if (balance > 0) {
//       return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
//     }

//     // ---------- FETCH CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // --- PDF Setup ---
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

//     const doc = new PDFDocument({ size: "A4", margin: 36 });
//     doc.pipe(res);

//     // register local font if available (optional)
//     try {
//       const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
//       if (fs && fs.existsSync(workSansPath)) {
//         doc.registerFont("WorkSans", workSansPath);
//         doc.font("WorkSans");
//       } else {
//         doc.font("Helvetica");
//       }
//     } catch (e) {
//       doc.font("Helvetica");
//     }

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     // Header logos (try-catch because resources may not exist)
//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
//     try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//     try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

//     // Clinic header (from profile)
//     doc.fontSize(14).font("Helvetica-Bold").text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//     doc.fontSize(9).font("Helvetica").text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//     doc.text(`PAN : ${clinicPAN}   |   Reg. No: ${clinicRegNo}`, { align: "center", width: pageWidth });

//     y += 56;
//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 8;

//     // Invoice Title
//     doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//     doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//     y += 28;

//     // Invoice details line (Invoice No + Date) - date only DD.MM.YYYY
//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateOnly(bill.date || "");
//     doc.fontSize(9).font("Helvetica");
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     // Patient info (address & sex swapped)
//     const patientName = bill.patientName || "";
//     const sexText = bill.sex ? String(bill.sex) : "";
//     const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const addressText = bill.address || "";

//     doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
//     doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
//     doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 20;

//     // ---------- SERVICES / ITEMS TABLE (invoice-html-pdf style) ----------
//     const tableLeft = 36;
//     const colSrW = 24;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colAdjW = 60; // not used visually, kept to match original structure
//     const colSubW = 80;
//     const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colAdjW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colAdjX = colRateX + colRateW;
//     const colSubX = colAdjX + colAdjW;
//     const tableRightX = tableLeft + usableWidth;

//     // layout constants
//     const headerHeight = 16;
//     const rowHeight = 14;
//     const minTableHeight = 200;
//     const bottomSafety = 120;

//     // helper to draw vertical separators for a vertical segment (on current page)
//     function drawVerticalsForSegmentItems(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colAdjX, colSubX, tableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // remember table start y so we can enforce min height later
//     const tableStartY = y;

//     // header background + border
//     doc
//       .save()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .fill("#F3F3F3")
//       .restore()
//       .rect(tableLeft, y, usableWidth, headerHeight)
//       .stroke();

//     // draw vertical separators for header (same as body)
//     drawVerticalsForSegmentItems(y, headerHeight);

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 2, y + 3);
//     doc.text("Procedure", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//     doc.text("Qty", colQtyX + 2, y + 3);
//     doc.text("Rate / Price", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//     doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });

//     y += headerHeight;

//     // draw a single horizontal separator line just below the header
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // We will accumulate a single vertical segment per page for items
//     let segmentStartY = y;
//     let segmentHeight = 0;

//     doc.font("Helvetica").fontSize(9);

//     for (let i = 0; i < items.length; i++) {
//       const it = items[i];

//       // dynamic calculation like invoice (we keep fixed row height but allow height based on text)
//       const rowPadding = 6;
//       const descH = doc.heightOfString(it.description || "", { width: colServiceW - 4 });
//       const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 4 });
//       const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 4 });
//       const amountH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 4 });

//       const rowTextMaxH = Math.max(descH, qtyH, rateH, amountH);
//       const thisRowH = Math.max(rowHeight, rowTextMaxH + rowPadding);

//       // page break check (reserve bottomSafety)
//       if (y + thisRowH > doc.page.height - bottomSafety) {
//         // draw vertical separators for current segment on this page
//         if (segmentHeight > 0) {
//           drawVerticalsForSegmentItems(segmentStartY, segmentHeight);
//         }

//         doc.addPage();
//         y = 36;

//         // redraw header area (logos optional)
//         try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//         try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//         doc.font("Helvetica-Bold").fontSize(14).text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//         y += 56;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 8;

//         // redraw title and table header on new page
//         doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//         doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//         y += 28;

//         // invoice details lines on new page are not necessary; jump to header table
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//         drawVerticalsForSegmentItems(y, headerHeight);
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 2, y + 3);
//         doc.text("Procedure", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//         doc.text("Qty", colQtyX + 2, y + 3);
//         doc.text("Rate / Price", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//         doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//         y += headerHeight;
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset segment tracking
//         segmentStartY = y;
//         segmentHeight = 0;
//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw row box and content
//       doc.rect(tableLeft, y, usableWidth, thisRowH).stroke();

//       const descY = y + (thisRowH - descH) / 2;
//       const qtyY = y + (thisRowH - qtyH) / 2;
//       const rateY = y + (thisRowH - rateH) / 2;
//       const amountY = y + (thisRowH - amountH) / 2;

//       doc.text(String(i + 1), colSrX + 2, y + 3);
//       doc.text(it.description || "", colServiceX + 2, descY, { width: colServiceW - 4 });
//       doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), colQtyX + 2, qtyY, { width: colQtyW - 4, align: "left" });
//       doc.text(formatMoney(it.rate || 0), colRateX + 2, rateY, { width: colRateW - 4, align: "right" });
//       doc.text(formatMoney(it.amount || 0), colSubX + 2, amountY, { width: colSubW - 4, align: "right" });

//       y += thisRowH;
//       segmentHeight += thisRowH;
//     }

//     // After drawing items, ensure minimum visual table height (fill empty rows)
//     const currentTableHeight = y - tableStartY;
//     if (currentTableHeight < minTableHeight) {
//       let remaining = minTableHeight - currentTableHeight;
//       const emptyRows = Math.ceil(remaining / rowHeight);
//       for (let i = 0; i < emptyRows; i++) {
//         if (y + rowHeight > doc.page.height - bottomSafety) {
//           if (segmentHeight > 0) {
//             drawVerticalsForSegmentItems(segmentStartY, segmentHeight);
//           }
//           doc.addPage();
//           y = 36;

//           // redraw condensed header area and table header on new page
//           try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//           try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//           doc.font("Helvetica-Bold").fontSize(14).text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//           doc.font("Helvetica").fontSize(9).text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//           y += 56;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 8;

//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore().rect(tableLeft, y, usableWidth, headerHeight).stroke();
//           drawVerticalsForSegmentItems(y, headerHeight);
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 2, y + 3);
//           doc.text("Procedure", colServiceX + 2, y + 3, { width: colServiceW - 4 });
//           doc.text("Qty", colQtyX + 2, y + 3);
//           doc.text("Rate / Price", colRateX + 2, y + 3, { width: colRateW - 4, align: "right" });
//           doc.text("Amount", colSubX + 2, y + 3, { width: colSubW - 4, align: "right" });
//           y += headerHeight;
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           segmentStartY = y;
//           segmentHeight = 0;
//         }

//         // filler row
//         doc.rect(tableLeft, y, usableWidth, rowHeight).stroke();
//         y += rowHeight;
//         segmentHeight += rowHeight;
//       }
//     }

//     // finally, draw vertical separators for the last page's items segment
//     if (segmentHeight > 0) {
//       drawVerticalsForSegmentItems(segmentStartY, segmentHeight);
//     }

//     // --- single horizontal separator after items before payments/totals
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     y += 12;

//     // ---------- PAYMENT DETAILS (chronological) (invoice style with verticals) ----------
//     const pTableLeft = 36;
//     const pColDateW = 100; // date/time area
//     const pColRecW = 140;
//     const pColModeW = 90;
//     const pColBankW = 110;
//     const pColRefW = 110;
//     const pColAmtW = usableWidth - (pColDateW + pColRecW + pColModeW + pColBankW + pColRefW);

//     const pColDateX = pTableLeft;
//     const pColRecX = pColDateX + pColDateW;
//     const pColModeX = pColRecX + pColRecW;
//     const pColBankX = pColModeX + pColModeW;
//     const pColRefX = pColBankX + pColBankW;
//     const pColAmtX = pColRefX + pColRefW;
//     const pTableRightX = pTableLeft + usableWidth;

//     const pHeaderH = 18;
//     const pMinRowH = 16;
//     const pSegmentBottomSafety = 120;

//     function drawVerticalsForSegmentPayments(yTop, height) {
//       const xs = [pColDateX, pColRecX, pColModeX, pColBankX, pColRefX, pColAmtX, pTableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // payments header
//     const paymentsTableStartY = y;
//     doc.save().rect(pTableLeft, y, usableWidth, pHeaderH).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, pHeaderH).stroke();
//     drawVerticalsForSegmentPayments(y, pHeaderH);

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//     doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//     doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//     doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//     doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//     doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });

//     y += pHeaderH;
//     doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

//     // accumulate vertical segment per page for payments
//     let pSegmentStartY = y;
//     let pSegmentHeight = 0;

//     doc.font("Helvetica").fontSize(9);

//     for (const p of payments) {
//       const dateTextP = formatDateOnly(p.paymentDateTime || p.paymentDate || `${p.paymentDate || ""}`);
//       const receiptText = p.receiptNo || p.id || "";
//       let modeText = p.mode || "-";
//       if ((modeText === "BankTransfer" || (modeText && modeText.toLowerCase().includes("bank"))) && p.transferType) {
//         modeText = `Bank (${p.transferType})`;
//       }
//       const bankText = p.bankName || "-";
//       const refText = p.referenceNo || "-";
//       const amtText = formatMoney(p.amount || 0);

//       // compute heights
//       const dH = doc.heightOfString(dateTextP, { width: pColDateW - 8 });
//       const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
//       const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
//       const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
//       const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
//       const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

//       const maxH = Math.max(dH, rH, mH, bH, refH, aH);
//       const rowH = Math.max(pMinRowH, maxH + 8);

//       // page break check
//       if (y + rowH > doc.page.height - pSegmentBottomSafety) {
//         if (pSegmentHeight > 0) {
//           drawVerticalsForSegmentPayments(pSegmentStartY, pSegmentHeight);
//         }
//         doc.addPage();
//         y = 36;

//         // redraw header area and payments header
//         try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//         try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//         doc.font("Helvetica-Bold").fontSize(14).text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//         y += 56;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 8;

//         // redraw payments header on new page
//         const headerTopY2 = y;
//         doc.save().rect(pTableLeft, headerTopY2, usableWidth, pHeaderH).fill("#F3F3F3").restore().rect(pTableLeft, headerTopY2, usableWidth, pHeaderH).stroke();
//         drawVerticalsForSegmentPayments(headerTopY2, pHeaderH);
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Date", pColDateX + 4, headerTopY2 + 4, { width: pColDateW - 8 });
//         doc.text("Receipt No.", pColRecX + 4, headerTopY2 + 4, { width: pColRecW - 8 });
//         doc.text("Mode", pColModeX + 4, headerTopY2 + 4, { width: pColModeW - 8 });
//         doc.text("Bank Name", pColBankX + 4, headerTopY2 + 4, { width: pColBankW - 8 });
//         doc.text("Reference", pColRefX + 4, headerTopY2 + 4, { width: pColRefW - 8 });
//         doc.text("Amount", pColAmtX + 4, headerTopY2 + 4, { width: pColAmtW - 8, align: "right" });
//         y += pHeaderH;
//         doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

//         // reset tracking
//         pSegmentStartY = y;
//         pSegmentHeight = 0;
//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw payment row box
//       doc.rect(pTableLeft, y, usableWidth, rowH).stroke();

//       const dateY = y + (rowH - dH) / 2;
//       const recY = y + (rowH - rH) / 2;
//       const modeY = y + (rowH - mH) / 2;
//       const bankY = y + (rowH - bH) / 2;
//       const refY = y + (rowH - refH) / 2;
//       const amtY = y + (rowH - aH) / 2;

//       doc.text(dateTextP, pColDateX + 4, dateY, { width: pColDateW - 8 });
//       doc.text(receiptText, pColRecX + 4, recY, { width: pColRecW - 8 });
//       doc.text(modeText, pColModeX + 4, modeY, { width: pColModeW - 8 });
//       doc.text(bankText, pColBankX + 4, bankY, { width: pColBankW - 8 });
//       doc.text(refText, pColRefX + 4, refY, { width: pColRefW - 8 });
//       doc.text(amtText, pColAmtX + 4, amtY, { width: pColAmtW - 8, align: "right" });

//       y += rowH;
//       pSegmentHeight += rowH;
//     }

//     // draw verticals for last payment segment
//     if (pSegmentHeight > 0) {
//       drawVerticalsForSegmentPayments(pSegmentStartY, pSegmentHeight);
//     }

//     y += 12;

//     // ---------- TOTALS BOX (placed on right like invoice) ----------
//     const boxWidth2 = 260;
//     const boxX2 = pageWidth - 36 - boxWidth2;
//     const boxY2 = y;
//     const lineH = 14;
//     const rowsCount = 5;
//     const boxHeight2 = rowsCount * lineH + 8;

//     // If not enough space, add new page
//     if (boxY2 + boxHeight2 + 60 > doc.page.height) {
//       doc.addPage();
//       y = 36;
//     }

//     doc.rect(boxX2, boxY2, boxWidth2, boxHeight2).stroke();
//     let by = boxY2 + 6;
//     doc.font("Helvetica").fontSize(9);

//     const addRow = (label, value) => {
//       doc.text(label, boxX2 + 6, by);
//       doc.text(value, boxX2 + 6, by, { width: boxWidth2 - 12, align: "right" });
//       by += lineH;
//     };

//     addRow("Total", `Rs ${formatMoney(total)}`);
//     addRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     addRow("Net Paid (after refunds)", `Rs ${formatMoney(netPaid)}`);
//     addRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     addRow("Balance", `Rs ${formatMoney(balance)}`);

//     y = boxY2 + boxHeight2 + 20;

//     // footer note + signatures (same as invoice)
//     doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
//     const sigY = y + 28;
//     const sigWidth = 160;
//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("full-payment-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate full payment PDF" });
//     } else {
//       try { res.end(); } catch (e) {}
//     }
//   }
// });


// app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   function formatMoney(v) { return Number(v || 0).toFixed(2); }

//   function formatDateOnly(dtString) {
//     return typeof formatDateDot === "function" ? formatDateDot(dtString) : dtString || "";
//   }

//   try {
//     // load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
//     const bill = billSnap.data();

//     // fetch items (legacy/new combined)
//     const itemsSnap = await db.collection("items").where("billId", "==", id).get();
//     const legacyItems = itemsSnap.docs.map((d) => {
//       const dd = d.data();
//       return {
//         id: d.id,
//         description: dd.description || dd.item || dd.details || "",
//         qty: Number(dd.qty || 0),
//         rate: Number(dd.rate || 0),
//         amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
//       };
//     });

//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;
//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);
//           return {
//             id: `svc-${idx + 1}`,
//             description: parts.join(" - "),
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // payments & refunds
//     const paysSnap = await db.collection("payments").where("billId", "==", id).get();
//     const payments = paysSnap.docs.map((d) => {
//       const pd = d.data();
//       return {
//         id: d.id,
//         paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
//         paymentDate: pd.paymentDate || null,
//         paymentTime: pd.paymentTime || null,
//         amount: Number(pd.amount || 0),
//         mode: pd.mode || "",
//         referenceNo: pd.referenceNo || "",
//         chequeDate: pd.chequeDate || null,
//         chequeNumber: pd.chequeNumber || null,
//         bankName: pd.bankName || null,
//         transferType: pd.transferType || null,
//         transferDate: pd.transferDate || null,
//         upiName: pd.upiName || null,
//         upiId: pd.upiId || null,
//         upiDate: pd.upiDate || null,
//         drawnOn: pd.drawnOn || null,
//         drawnAs: pd.drawnAs || null,
//         receiptNo: pd.receiptNo || d.id,
//       };
//     });

//     const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
//     const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

//     // sort payments chronologically
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     // totals
//     const total = Number(bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0));
//     const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
//     const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = total - netPaid;

//     // Only allow full-payment PDF if balance is zero (or less)
//     if (balance > 0) {
//       return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
//     }

//     // ---------- FETCH CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // --- PDF Setup ---
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

//     const doc = new PDFDocument({ size: "A4", margin: 36 });
//     doc.pipe(res);

//     // register local font if available (optional)
//     try {
//       const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
//       if (fs && fs.existsSync(workSansPath)) {
//         doc.registerFont("WorkSans", workSansPath);
//         doc.font("WorkSans");
//       } else {
//         doc.font("Helvetica");
//       }
//     } catch (e) {
//       doc.font("Helvetica");
//     }

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     // Header logos (try-catch because resources may not exist)
//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
//     try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//     try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

//     // Clinic header (from profile)
//     doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//     doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//     doc.text(`PAN : ${clinicPAN || ""}   |   Reg. No: ${clinicRegNo || ""}`, { align: "center", width: pageWidth });

//     y += 56;
//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 8;

//     // Invoice Title
//     doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//     doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//     y += 28;

//     // Invoice details line (Invoice No + Date) - date only DD.MM.YYYY
//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateOnly(bill.date || "");
//     doc.fontSize(9).font("Helvetica");
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     // Patient info (address & sex swapped)
//     const patientName = bill.patientName || "";
//     const sexText = bill.sex ? String(bill.sex) : "";
//     const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const addressText = bill.address || "";

//     doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
//     doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
//     doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 20;

//     // ---------- SERVICES / ITEMS TABLE (outer-border + vertical separators like image) ----------
//     const tableLeft = 36;
//     const colSrW = 24;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 80;
//     const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     const headerHeight = 16;
//     const minRowH = 14;
//     const minTableHeight = 260; // increase so description area becomes large like image
//     const bottomSafety = 120;

//     // helper to draw vertical separators across full table block
//     function drawVerticalsForItemsBlock(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       xs.forEach((x) => {
//         doc.moveTo(x, yTop).lineTo(x, yTop + height).stroke();
//       });
//     }

//     const tableStartY = y;

//     // header background + border top
//     doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//     // don't draw the outer border yet - we'll draw it after we've decided final height
//     // render header texts
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 4, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//     doc.text("Qty", colQtyX + 4, y + 3);
//     doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//     doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });

//     y += headerHeight;

//     // single horizontal separator under header
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // render items' content (only text, NO horizontal separators per row)
//     doc.font("Helvetica").fontSize(9);
//     let filledHeight = 0;
//     const rowsMeta = []; // keep heights to possibly compute final block height
//     for (let i = 0; i < items.length; i++) {
//       const it = items[i];
//       const descH = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
//       const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
//       const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
//       const amtH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });

//       const contentMaxH = Math.max(descH, qtyH, rateH, amtH);
//       const thisRowH = Math.max(minRowH, contentMaxH + 8);

//       // page break if necessary BEFORE placing row
//       if (y + thisRowH > doc.page.height - bottomSafety) {
//         // finalize verticals for segment before page break
//         const segmentHeight = filledHeight;
//         if (segmentHeight > 0) {
//           // draw verticals for previous page segment
//           drawVerticalsForItemsBlock(tableStartY, segmentHeight);
//           // draw outer border for that page's table area
//           doc.rect(tableLeft, tableStartY, usableWidth, segmentHeight + headerHeight).stroke();
//         }

//         doc.addPage();
//         y = 36;

//         // redraw header area on new page
//         try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//         try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//         doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//         doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//         y += 56;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 8;

//         doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//         doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//         y += 28;

//         // re-render header for the table on new page
//         const newTableStartY = y;
//         // header background
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 4, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//         doc.text("Qty", colQtyX + 4, y + 3);
//         doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//         doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
//         y += headerHeight;
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         // reset tableStartY and filledHeight for new page
//         filledHeight = 0;
//         rowsMeta.length = 0;
//         // update tableStartY for the new page (we will draw verticals after filling)
//         tableStartY = y - headerHeight; // headerTopY for new page
//       }

//       // draw text into columns (centered vertically within row)
//       const rowTop = y;
//       const descH2 = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
//       const qtyH2 = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
//       const rateH2 = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
//       const amtH2 = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });
//       const actualRowH = Math.max(minRowH, Math.max(descH2, qtyH2, rateH2, amtH2) + 8);

//       const descY = rowTop + (actualRowH - descH2) / 2;
//       const qtyY = rowTop + (actualRowH - qtyH2) / 2;
//       const rateY = rowTop + (actualRowH - rateH2) / 2;
//       const amtY = rowTop + (actualRowH - amtH2) / 2;

//       doc.text(String(i + 1), colSrX + 4, rowTop + 4);
//       doc.text(it.description || "", colServiceX + 4, descY, { width: colServiceW - 8 });
//       doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), colQtyX + 4, qtyY, { width: colQtyW - 8, align: "left" });
//       doc.text(formatMoney(it.rate || 0), colRateX + 4, rateY, { width: colRateW - 8, align: "right" });
//       doc.text(formatMoney(it.amount || 0), colSubX + 4, amtY, { width: colSubW - 8, align: "right" });

//       y += actualRowH;
//       filledHeight += actualRowH;
//       rowsMeta.push({ top: rowTop, height: actualRowH });
//     }

//     // After items drawn on the current page: ensure table has at least minTableHeight visual area
//     let totalTableHeight = filledHeight; // height for the content area (below header)
//     if (totalTableHeight < minTableHeight) {
//       const needed = minTableHeight - totalTableHeight;
//       // add filler rows (visual empty space) - just advance y and increase filledHeight
//       const fillerUnits = Math.ceil(needed / minRowH);
//       for (let f = 0; f < fillerUnits; f++) {
//         // page break safety while filling
//         if (y + minRowH > doc.page.height - bottomSafety) {
//           // finalize verticals and border for current page's table block
//           drawVerticalsForItemsBlock(tableStartY, filledHeight + headerHeight);
//           doc.rect(tableLeft, tableStartY, usableWidth, filledHeight + headerHeight).stroke();

//           doc.addPage();
//           y = 36;

//           // redraw header area on new page and set new tableStartY
//           try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//           try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//           doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//           doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//           y += 56;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 8;

//           doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//           doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//           y += 28;

//           // render header again
//           tableStartY = y;
//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 4, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//           doc.text("Qty", colQtyX + 4, y + 3);
//           doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//           doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
//           y += headerHeight;
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           filledHeight = 0;
//         }

//         // add filler block
//         y += minRowH;
//         filledHeight += minRowH;
//       }
//       totalTableHeight = filledHeight;
//     }

//     // Now draw vertical separators spanning the full visual table block (header + content)
//     const visualTableHeight = headerHeight + totalTableHeight;
//     drawVerticalsForItemsBlock(tableStartY, visualTableHeight);

//     // draw outer border around the whole table block (header + content)
//     doc.rect(tableLeft, tableStartY, usableWidth, visualTableHeight).stroke();

//     // Place totals box so that it visually attaches to bottom-right of the table block (like image)
//     const boxWidth = 180;
//     const boxHeight = 36; // two rows height visually
//     // Make the top of the box align exactly with bottom edge of table (so box sits immediately under it)
//     // to mimic the screenshot where totals box is attached to the table bottom-right, we set boxY just below table
//     const boxX = tableRightX - boxWidth;
//     const boxY = tableStartY + visualTableHeight - boxHeight / 2; // slight overlap for visual attachment

//     // Ensure box fits on current page, otherwise add new page and recalc positions
//     if (boxY + boxHeight + 60 > doc.page.height) {
//       doc.addPage();
//       // place box at top area on new page
//       // redraw small header block for context
//       try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, 36, { width: 40, height: 40 }); } catch (e) {}
//       try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, 36, { width: 40, height: 40 }); } catch (e) {}
//       doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, 42, { align: "center", width: pageWidth });
//       y = 100;
//       // place box in new page's upper area
//       doc.rect(36, y, boxWidth, boxHeight).stroke();
//       doc.font("Helvetica-Bold").fontSize(9).text("Total Due", 36 + 6, y + 6);
//       doc.font("Helvetica").fontSize(9).text(`Rs ${formatMoney(total)}`, 36, y + 6, { width: boxWidth - 8, align: "right" });
//       y += boxHeight + 20;
//     } else {
//       // draw totals box attached to table bottom-right
//       doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();
//       doc.font("Helvetica-Bold").fontSize(9).text("Total Due", boxX + 6, boxY + 6);
//       doc.font("Helvetica").fontSize(9).text(`Rs ${formatMoney(total)}`, boxX, boxY + 6, { width: boxWidth - 8, align: "right" });
//       // advance y below box for footer
//       y = tableStartY + visualTableHeight + 20;
//     }

//     // FOOTER NOTE + SIGNATURE
//     doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
//     const sigY = y + 28;
//     const sigWidth = 160;
//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("full-payment-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate full payment PDF" });
//     } else {
//       try { res.end(); } catch (e) {}
//     }
//   }
// });


// app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
//   const id = req.params.id;
//   if (!id) return res.status(400).json({ error: "Invalid bill id" });

//   function formatMoney(v) { return Number(v || 0).toFixed(2); }

//   function formatDateOnly(dtString) {
//     return typeof formatDateDot === "function" ? formatDateDot(dtString) : dtString || "";
//   }

//   try {
//     // load bill
//     const billRef = db.collection("bills").doc(id);
//     const billSnap = await billRef.get();
//     if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
//     const bill = billSnap.data();

//     // fetch items (legacy/new combined)
//     const itemsSnap = await db.collection("items").where("billId", "==", id).get();
//     const legacyItems = itemsSnap.docs.map((d) => {
//       const dd = d.data();
//       return {
//         id: d.id,
//         description: dd.description || dd.item || dd.details || "",
//         qty: Number(dd.qty || 0),
//         rate: Number(dd.rate || 0),
//         amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
//       };
//     });

//     const serviceItems = Array.isArray(bill.services)
//       ? bill.services.map((s, idx) => {
//           const qty = Number(s.qty || 0);
//           const rate = Number(s.rate || 0);
//           const amount = s.amount != null ? Number(s.amount) : qty * rate;
//           const parts = [];
//           if (s.item) parts.push(s.item);
//           if (s.details) parts.push(s.details);
//           return {
//             id: `svc-${idx + 1}`,
//             description: parts.join(" - "),
//             qty,
//             rate,
//             amount,
//           };
//         })
//       : [];

//     const items = serviceItems.length > 0 ? serviceItems : legacyItems;

//     // payments & refunds
//     const paysSnap = await db.collection("payments").where("billId", "==", id).get();
//     const payments = paysSnap.docs.map((d) => {
//       const pd = d.data();
//       return {
//         id: d.id,
//         paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
//         paymentDate: pd.paymentDate || null,
//         paymentTime: pd.paymentTime || null,
//         amount: Number(pd.amount || 0),
//         mode: pd.mode || "",
//         referenceNo: pd.referenceNo || "",
//         chequeDate: pd.chequeDate || null,
//         chequeNumber: pd.chequeNumber || null,
//         bankName: pd.bankName || null,
//         transferType: pd.transferType || null,
//         transferDate: pd.transferDate || null,
//         upiName: pd.upiName || null,
//         upiId: pd.upiId || null,
//         upiDate: pd.upiDate || null,
//         drawnOn: pd.drawnOn || null,
//         drawnAs: pd.drawnAs || null,
//         receiptNo: pd.receiptNo || d.id,
//       };
//     });

//     const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
//     const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

//     // sort payments chronologically
//     payments.sort((a, b) => {
//       const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
//       const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
//       return da - dbb;
//     });

//     // totals
//     const total = Number(bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0));
//     const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
//     const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
//     const netPaid = totalPaidGross - totalRefunded;
//     const balance = total - netPaid;

//     // Only allow full-payment PDF if balance is zero (or less)
//     if (balance > 0) {
//       return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
//     }

//     // ---------- FETCH CLINIC PROFILE ----------
//     const profile = await getClinicProfile({ force: true });
//     const clinicName = profileValue(profile, "clinicName");
//     const clinicAddress = profileValue(profile, "address");
//     const clinicPAN = profileValue(profile, "pan");
//     const clinicRegNo = profileValue(profile, "regNo");
//     const doctor1Name = profileValue(profile, "doctor1Name");
//     const doctor1RegNo = profileValue(profile, "doctor1RegNo");
//     const doctor2Name = profileValue(profile, "doctor2Name");
//     const doctor2RegNo = profileValue(profile, "doctor2RegNo");
//     const patientRepresentative = profileValue(profile, "patientRepresentative");
//     const clinicRepresentative = profileValue(profile, "clinicRepresentative");

//     // --- PDF Setup ---
//     res.setHeader("Content-Type", "application/pdf");
//     res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

//     const doc = new PDFDocument({ size: "A4", margin: 36 });
//     doc.pipe(res);

//     // register local font if available (optional)
//     try {
//       const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
//       if (fs && fs.existsSync(workSansPath)) {
//         doc.registerFont("WorkSans", workSansPath);
//         doc.font("WorkSans");
//       } else {
//         doc.font("Helvetica");
//       }
//     } catch (e) {
//       doc.font("Helvetica");
//     }

//     const pageWidth = doc.page.width;
//     const usableWidth = pageWidth - 72;
//     let y = 36;

//     // Header logos (try-catch because resources may not exist)
//     const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
//     const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
//     try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//     try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

//     // Clinic header (from profile)
//     doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//     doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//     doc.text(`PAN : ${clinicPAN || ""}   |   Reg. No: ${clinicRegNo || ""}`, { align: "center", width: pageWidth });

//     y += 56;
//     doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//     y += 8;

//     // Invoice Title
//     doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//     doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//     y += 28;

//     // Invoice details line (Invoice No + Date) - date only DD.MM.YYYY
//     const invoiceNo = bill.invoiceNo || id;
//     const dateText = formatDateOnly(bill.date || "");
//     doc.fontSize(9).font("Helvetica");
//     doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
//     doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     // Patient info (address & sex swapped)
//     const patientName = bill.patientName || "";
//     const sexText = bill.sex ? String(bill.sex) : "";
//     const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
//     const addressText = bill.address || "";

//     doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
//     doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 14;

//     doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
//     doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
//     y += 20;

//     // ---------- SERVICES / ITEMS TABLE (outer-border + vertical separators like image) ----------
//     const tableLeft = 36;
//     const colSrW = 24;
//     const colQtyW = 48;
//     const colRateW = 70;
//     const colSubW = 80;
//     const colServiceW = usableWidth - (colSrW + colQtyW + colRateW + colSubW);

//     const colSrX = tableLeft;
//     const colServiceX = colSrX + colSrW;
//     const colQtyX = colServiceX + colServiceW;
//     const colRateX = colQtyX + colQtyW;
//     const colSubX = colRateX + colRateW;
//     const tableRightX = tableLeft + usableWidth;

//     const headerHeight = 16;
//     const minRowH = 14;
//     const minTableHeight = 260; // adjust for large description area
//     const bottomSafety = 120;

//     // helper to draw vertical separators across full table block
//     function drawVerticalsForItemsBlock(yTop, height) {
//       const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
//       xs.forEach((x) => {
//         doc.moveTo(x, yTop).lineTo(x, yTop + height).stroke();
//       });
//     }

//     // We'll set tableStartY to header top
//     let tableStartY = y;

//     // header background + text (don't draw outer border yet until we know final height)
//     doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Sr.", colSrX + 4, y + 3);
//     doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//     doc.text("Qty", colQtyX + 4, y + 3);
//     doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//     doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });

//     y += headerHeight;

//     // single horizontal separator under header
//     doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//     // render items (text only — no horizontal lines between rows; we'll draw verticals later spanning whole block)
//     doc.font("Helvetica").fontSize(9);
//     let filledHeight = 0;
//     for (let i = 0; i < items.length; i++) {
//       const it = items[i];

//       const descH = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
//       const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
//       const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
//       const amtH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });

//       const contentMaxH = Math.max(descH, qtyH, rateH, amtH);
//       const thisRowH = Math.max(minRowH, contentMaxH + 8);

//       // page-break check before placing row
//       if (y + thisRowH > doc.page.height - bottomSafety) {
//         // finalize verticals + border for the block drawn so far on this page
//         const visualHeightSoFar = headerHeight + filledHeight;
//         if (visualHeightSoFar > 0) {
//           drawVerticalsForItemsBlock(tableStartY, visualHeightSoFar);
//           doc.rect(tableLeft, tableStartY, usableWidth, visualHeightSoFar).stroke();
//         }

//         doc.addPage();
//         y = 36;

//         // redraw header area for new page
//         try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//         try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//         doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//         doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//         y += 56;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 8;

//         doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//         doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//         y += 28;

//         // render table header on new page and reset trackers
//         tableStartY = y;
//         doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Sr.", colSrX + 4, y + 3);
//         doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//         doc.text("Qty", colQtyX + 4, y + 3);
//         doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//         doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
//         y += headerHeight;
//         doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//         filledHeight = 0;
//       }

//       // draw row text (centered vertically within row)
//       const descH2 = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
//       const qtyH2 = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
//       const rateH2 = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
//       const amtH2 = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });
//       const actualRowH = Math.max(minRowH, Math.max(descH2, qtyH2, rateH2, amtH2) + 8);

//       const rowTop = y;
//       const descY = rowTop + (actualRowH - descH2) / 2;
//       const qtyY = rowTop + (actualRowH - qtyH2) / 2;
//       const rateY = rowTop + (actualRowH - rateH2) / 2;
//       const amtY = rowTop + (actualRowH - amtH2) / 2;

//       doc.text(String(i + 1), colSrX + 4, rowTop + 4);
//       doc.text(it.description || "", colServiceX + 4, descY, { width: colServiceW - 8 });
//       doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), colQtyX + 4, qtyY, { width: colQtyW - 8, align: "left" });
//       doc.text(formatMoney(it.rate || 0), colRateX + 4, rateY, { width: colRateW - 8, align: "right" });
//       doc.text(formatMoney(it.amount || 0), colSubX + 4, amtY, { width: colSubW - 8, align: "right" });

//       y += actualRowH;
//       filledHeight += actualRowH;
//     }

//     // ensure minimum visual height of the items block (adds filler rows if necessary)
//     let totalTableHeight = filledHeight;
//     if (totalTableHeight < minTableHeight) {
//       let needed = minTableHeight - totalTableHeight;
//       const fillerRows = Math.ceil(needed / minRowH);
//       for (let fr = 0; fr < fillerRows; fr++) {
//         if (y + minRowH > doc.page.height - bottomSafety) {
//           // finalize previous page table visuals
//           drawVerticalsForItemsBlock(tableStartY, headerHeight + totalTableHeight);
//           doc.rect(tableLeft, tableStartY, usableWidth, headerHeight + totalTableHeight).stroke();

//           doc.addPage();
//           y = 36;

//           // header context on new page
//           try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//           try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//           doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
//           doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
//           y += 56;
//           doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//           y += 8;

//           doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
//           doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
//           y += 28;

//           // render header again, reset trackers
//           tableStartY = y;
//           doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
//           doc.font("Helvetica-Bold").fontSize(9);
//           doc.text("Sr.", colSrX + 4, y + 3);
//           doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
//           doc.text("Qty", colQtyX + 4, y + 3);
//           doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
//           doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
//           y += headerHeight;
//           doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

//           totalTableHeight = 0;
//         }

//         // filler row
//         y += minRowH;
//         totalTableHeight += minRowH;
//       }
//       filledHeight = totalTableHeight;
//     }

//     // draw vertical separators spanning header + content
//     const visualTableHeight = headerHeight + filledHeight;
//     drawVerticalsForItemsBlock(tableStartY, visualTableHeight);

//     // draw outer border around the table block (header + content)
//     doc.rect(tableLeft, tableStartY, usableWidth, visualTableHeight).stroke();

//     // ===== Place Total Due box IMMEDIATELY AFTER TABLE (no gap) =====
//     const boxWidth = 180;
//     const boxHeight = 36; // height of totals box
//     const boxX = tableRightX - boxWidth;
//     // place top of box exactly at bottom edge of table (no gap)
//     let boxY = tableStartY + visualTableHeight;

//     // If the box doesn't fit on the current page, add a new page and place it near top (we keep payment details intact)
//     if (boxY + boxHeight + 60 > doc.page.height) {
//       doc.addPage();
//       // redraw small header area for context
//       try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, 36, { width: 40, height: 40 }); } catch (e) {}
//       try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, 36, { width: 40, height: 40 }); } catch (e) {}
//       doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, 42, { align: "center", width: pageWidth });
//       // place the box near top of new page (just below header area)
//       boxY = 90;
//     }

//     // draw the totals box attached to table bottom (or on new page if overflow)
//     doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();
//     doc.font("Helvetica-Bold").fontSize(9).text("Total Due", boxX + 6, boxY + 6);
//     doc.font("Helvetica").fontSize(9).text(`Rs ${formatMoney(total)}`, boxX, boxY + 6, { width: boxWidth - 8, align: "right" });

//     // set y to continue AFTER the box (if box was on same page below table, this keeps payment details after box)
//     if (boxY === tableStartY + visualTableHeight) {
//       // box placed immediately after table on same page
//       y = boxY + boxHeight + 8;
//     } else {
//       // box placed on new page near top, set y accordingly to place payment details below
//       y = boxY + boxHeight + 12;
//     }

//     // ---------- PAYMENT DETAILS (chronological) (kept as-is, appears after totals box) ----------
//     const pTableLeft = 36;
//     const pColDateW = 100; // date/time area
//     const pColRecW = 140;
//     const pColModeW = 90;
//     const pColBankW = 110;
//     const pColRefW = 110;
//     const pColAmtW = usableWidth - (pColDateW + pColRecW + pColModeW + pColBankW + pColRefW);

//     const pColDateX = pTableLeft;
//     const pColRecX = pColDateX + pColDateW;
//     const pColModeX = pColRecX + pColRecW;
//     const pColBankX = pColModeX + pColModeW;
//     const pColRefX = pColBankX + pColBankW;
//     const pColAmtX = pColRefX + pColRefW;
//     const pTableRightX = pTableLeft + usableWidth;

//     const pHeaderH = 18;
//     const pMinRowH = 16;
//     const pSegmentBottomSafety = 120;

//     function drawVerticalsForSegmentPayments(yTop, height) {
//       const xs = [pColDateX, pColRecX, pColModeX, pColBankX, pColRefX, pColAmtX, pTableRightX];
//       const top = yTop;
//       const bottom = yTop + height;
//       xs.forEach((x) => {
//         doc.moveTo(x, top).lineTo(x, bottom).stroke();
//       });
//     }

//     // payments header
//     const paymentsTableStartY = y;
//     doc.save().rect(pTableLeft, y, usableWidth, pHeaderH).fill("#F3F3F3").restore().rect(pTableLeft, y, usableWidth, pHeaderH).stroke();
//     drawVerticalsForSegmentPayments(y, pHeaderH);

//     doc.font("Helvetica-Bold").fontSize(9);
//     doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
//     doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
//     doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
//     doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
//     doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
//     doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });

//     y += pHeaderH;
//     doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

//     // accumulate vertical segment per page for payments
//     let pSegmentStartY = y;
//     let pSegmentHeight = 0;

//     doc.font("Helvetica").fontSize(9);

//     for (const p of payments) {
//       const dateTextP = formatDateOnly(p.paymentDateTime || p.paymentDate || `${p.paymentDate || ""}`);
//       const receiptText = p.receiptNo || p.id || "";
//       let modeText = p.mode || "-";
//       if ((modeText === "BankTransfer" || (modeText && modeText.toLowerCase().includes("bank"))) && p.transferType) {
//         modeText = `Bank (${p.transferType})`;
//       }
//       const bankText = p.bankName || "-";
//       const refText = p.referenceNo || "-";
//       const amtText = formatMoney(p.amount || 0);

//       // compute heights
//       const dH = doc.heightOfString(dateTextP, { width: pColDateW - 8 });
//       const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
//       const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
//       const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
//       const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
//       const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

//       const maxH = Math.max(dH, rH, mH, bH, refH, aH);
//       const rowH = Math.max(pMinRowH, maxH + 8);

//       // page break check
//       if (y + rowH > doc.page.height - pSegmentBottomSafety) {
//         if (pSegmentHeight > 0) {
//           drawVerticalsForSegmentPayments(pSegmentStartY, pSegmentHeight);
//         }
//         doc.addPage();
//         y = 36;

//         // redraw header area and payments header
//         try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
//         try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
//         doc.font("Helvetica-Bold").fontSize(14).text(clinicName, 0, y + 6, { align: "center", width: pageWidth });
//         doc.font("Helvetica").fontSize(9).text(clinicAddress, 0, y + 28, { align: "center", width: pageWidth });
//         y += 56;
//         doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
//         y += 8;

//         // redraw payments header on new page
//         const headerTopY2 = y;
//         doc.save().rect(pTableLeft, headerTopY2, usableWidth, pHeaderH).fill("#F3F3F3").restore().rect(pTableLeft, headerTopY2, usableWidth, pHeaderH).stroke();
//         drawVerticalsForSegmentPayments(headerTopY2, pHeaderH);
//         doc.font("Helvetica-Bold").fontSize(9);
//         doc.text("Date", pColDateX + 4, headerTopY2 + 4, { width: pColDateW - 8 });
//         doc.text("Receipt No.", pColRecX + 4, headerTopY2 + 4, { width: pColRecW - 8 });
//         doc.text("Mode", pColModeX + 4, headerTopY2 + 4, { width: pColModeW - 8 });
//         doc.text("Bank Name", pColBankX + 4, headerTopY2 + 4, { width: pColBankW - 8 });
//         doc.text("Reference", pColRefX + 4, headerTopY2 + 4, { width: pColRefW - 8 });
//         doc.text("Amount", pColAmtX + 4, headerTopY2 + 4, { width: pColAmtW - 8, align: "right" });
//         y += pHeaderH;
//         doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

//         // reset tracking
//         pSegmentStartY = y;
//         pSegmentHeight = 0;
//         doc.font("Helvetica").fontSize(9);
//       }

//       // draw payment row box
//       doc.rect(pTableLeft, y, usableWidth, rowH).stroke();

//       const dateY = y + (rowH - dH) / 2;
//       const recY = y + (rowH - rH) / 2;
//       const modeY = y + (rowH - mH) / 2;
//       const bankY = y + (rowH - bH) / 2;
//       const refY = y + (rowH - refH) / 2;
//       const amtY = y + (rowH - aH) / 2;

//       doc.text(dateTextP, pColDateX + 4, dateY, { width: pColDateW - 8 });
//       doc.text(receiptText, pColRecX + 4, recY, { width: pColRecW - 8 });
//       doc.text(modeText, pColModeX + 4, modeY, { width: pColModeW - 8 });
//       doc.text(bankText, pColBankX + 4, bankY, { width: pColBankW - 8 });
//       doc.text(refText, pColRefX + 4, refY, { width: pColRefW - 8 });
//       doc.text(amtText, pColAmtX + 4, amtY, { width: pColAmtW - 8, align: "right" });

//       y += rowH;
//       pSegmentHeight += rowH;
//     }

//     // draw verticals for last payment segment
//     if (pSegmentHeight > 0) {
//       drawVerticalsForSegmentPayments(pSegmentStartY, pSegmentHeight);
//     }

//     y += 12;

//     // ---------- FOOTER TOTALS BOX (kept if you want duplicate summary) ----------
//     // (Optional) You can keep or remove — I will keep small totals box at left as in previous layout
//     const boxWidth2 = 260;
//     const boxX2 = 36;
//     const boxY2 = y;
//     const lineH = 14;
//     const rowsCount = 5;
//     const boxHeight2 = rowsCount * lineH + 8;

//     // If not enough space, add new page
//     if (boxY2 + boxHeight2 + 60 > doc.page.height) {
//       doc.addPage();
//       y = 36;
//     }

//     doc.rect(boxX2, boxY2, boxWidth2, boxHeight2).stroke();
//     let by = boxY2 + 6;
//     doc.font("Helvetica").fontSize(9);

//     const addRow = (label, value) => {
//       doc.text(label, boxX2 + 6, by);
//       doc.text(value, boxX2 + 6, by, { width: boxWidth2 - 12, align: "right" });
//       by += lineH;
//     };

//     addRow("Total", `Rs ${formatMoney(total)}`);
//     addRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
//     addRow("Net Paid (after refunds)", `Rs ${formatMoney(netPaid)}`);
//     addRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
//     addRow("Balance", `Rs ${formatMoney(balance)}`);

//     y = boxY2 + boxHeight2 + 20;

//     // footer note + signatures (same as invoice)
//     doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
//     const sigY = y + 28;
//     const sigWidth = 160;
//     doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });
//     const rightSigX = pageWidth - 36 - sigWidth;
//     doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
//     doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, { width: sigWidth, align: "center" });

//     doc.end();
//   } catch (err) {
//     console.error("full-payment-pdf error:", err);
//     if (!res.headersSent) {
//       res.status(500).json({ error: "Failed to generate full payment PDF" });
//     } else {
//       try { res.end(); } catch (e) {}
//     }
//   }
// });

app.get("/api/bills/:id/full-payment-pdf", async (req, res) => {
  const id = req.params.id;
  if (!id) return res.status(400).json({ error: "Invalid bill id" });

  function formatMoney(v) { return Number(v || 0).toFixed(2); }

  function formatDateOnly(dtString) {
    return typeof formatDateDot === "function" ? formatDateDot(dtString) : dtString || "";
  }

  try {
    // load bill
    const billRef = db.collection("bills").doc(id);
    const billSnap = await billRef.get();
    if (!billSnap.exists) return res.status(404).json({ error: "Bill not found" });
    const bill = billSnap.data();

    // fetch items (legacy/new combined)
    const itemsSnap = await db.collection("items").where("billId", "==", id).get();
    const legacyItems = itemsSnap.docs.map((d) => {
      const dd = d.data();
      return {
        id: d.id,
        description: dd.description || dd.item || dd.details || "",
        qty: Number(dd.qty || 0),
        rate: Number(dd.rate || 0),
        amount: dd.amount != null ? Number(dd.amount) : Number(dd.qty || 0) * Number(dd.rate || 0),
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
    const paysSnap = await db.collection("payments").where("billId", "==", id).get();
    const payments = paysSnap.docs.map((d) => {
      const pd = d.data();
      return {
        id: d.id,
        paymentDateTime: pd.paymentDateTime || (pd.paymentDate ? `${pd.paymentDate}T${pd.paymentTime || "00:00"}:00.000Z` : null),
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

    const refundsSnap = await db.collection("refunds").where("billId", "==", id).get();
    const refunds = refundsSnap.docs.map((d) => ({ id: d.id, ...d.data() }));

    // sort payments chronologically
    payments.sort((a, b) => {
      const da = a.paymentDateTime ? new Date(a.paymentDateTime) : new Date(0);
      const dbb = b.paymentDateTime ? new Date(b.paymentDateTime) : new Date(0);
      return da - dbb;
    });

    // totals
    const total = Number(bill.total || items.reduce((s, it) => s + Number(it.amount || 0), 0));
    const totalPaidGross = payments.reduce((s, p) => s + Number(p.amount || 0), 0);
    const totalRefunded = refunds.reduce((s, r) => s + Number(r.amount || 0), 0);
    const netPaid = totalPaidGross - totalRefunded;
    const balance = total - netPaid;

    // Only allow full-payment PDF if balance is zero (or less)
    if (balance > 0) {
      return res.status(400).json({ error: "Bill not fully paid - full payment PDF is available only after full payment" });
    }

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
    const patientRepresentative = profileValue(profile, "patientRepresentative");
    const clinicRepresentative = profileValue(profile, "clinicRepresentative");

    // --- PDF Setup ---
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="full-payment-${id}.pdf"`);

    const doc = new PDFDocument({ size: "A4", margin: 36 });
    doc.pipe(res);

    // register local font if available (optional)
    try {
      const workSansPath = path.join(__dirname, "resources", "WorkSans-Regular.ttf");
      if (fs && fs.existsSync(workSansPath)) {
        doc.registerFont("WorkSans", workSansPath);
        doc.font("WorkSans");
      } else {
        doc.font("Helvetica");
      }
    } catch (e) {
      doc.font("Helvetica");
    }

    const pageWidth = doc.page.width;
    const usableWidth = pageWidth - 72;
    let y = 36;

    // Header logos (try-catch because resources may not exist)
    const logoLeftPath = path.join(__dirname, "resources", "logo-left.png");
    const logoRightPath = path.join(__dirname, "resources", "logo-right.png");
    try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
    try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}

    // Clinic header (from profile)
    doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
    doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
    doc.text(`PAN : ${clinicPAN || ""}   |   Reg. No: ${clinicRegNo || ""}`, { align: "center", width: pageWidth });

    y += 56;
    doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
    y += 8;

    // Invoice Title
    doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
    doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
    y += 28;

    // Invoice details line (Invoice No + Date) - date only DD.MM.YYYY
    const invoiceNo = bill.invoiceNo || id;
    const dateText = formatDateOnly(bill.date || "");
    doc.fontSize(9).font("Helvetica");
    doc.text(`Invoice No.: ${invoiceNo}`, 36, y);
    doc.text(`Date: ${dateText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
    y += 14;

    // Patient info (address & sex swapped)
    const patientName = bill.patientName || "";
    const sexText = bill.sex ? String(bill.sex) : "";
    const ageText = bill.age != null && bill.age !== "" ? `${bill.age} Years` : "";
    const addressText = bill.address || "";

    doc.font("Helvetica-Bold").text(`Patient Name: ${patientName}`, 36, y);
    doc.font("Helvetica").text(`Age: ${ageText}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
    y += 14;

    doc.font("Helvetica").text(`Address: ${addressText || "____________________"}`, 36, y, { width: usableWidth * 0.6 });
    doc.font("Helvetica").text(`Sex: ${sexText || "-"}`, pageWidth / 2, y, { width: usableWidth / 2, align: "right" });
    y += 20;

    // ---------- SERVICES / ITEMS TABLE (outer-border + vertical separators like image) ----------
    const tableLeft = 36;
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

    const headerHeight = 16;
    const minRowH = 14;
    const minTableHeight = 260; // adjust for large description area
    const bottomSafety = 120;

    // helper to draw vertical separators across full table block
    function drawVerticalsForItemsBlock(yTop, height) {
      const xs = [colSrX, colServiceX, colQtyX, colRateX, colSubX, tableRightX];
      xs.forEach((x) => {
        doc.moveTo(x, yTop).lineTo(x, yTop + height).stroke();
      });
    }

    // We'll set tableStartY to header top
    let tableStartY = y;

    // header background + text (don't draw outer border yet until we know final height)
    doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Sr.", colSrX + 4, y + 3);
    doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
    doc.text("Qty", colQtyX + 4, y + 3);
    doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
    doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });

    y += headerHeight;

    // single horizontal separator under header
    doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

    // render items (text only — no horizontal lines between rows; we'll draw verticals later spanning whole block)
    doc.font("Helvetica").fontSize(9);
    let filledHeight = 0;
    for (let i = 0; i < items.length; i++) {
      const it = items[i];

      const descH = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
      const qtyH = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
      const rateH = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
      const amtH = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });

      const contentMaxH = Math.max(descH, qtyH, rateH, amtH);
      const thisRowH = Math.max(minRowH, contentMaxH + 8);

      // page-break check before placing row
      if (y + thisRowH > doc.page.height - bottomSafety) {
        // finalize verticals + border for the block drawn so far on this page
        const visualHeightSoFar = headerHeight + filledHeight;
        if (visualHeightSoFar > 0) {
          drawVerticalsForItemsBlock(tableStartY, visualHeightSoFar);
          doc.rect(tableLeft, tableStartY, usableWidth, visualHeightSoFar).stroke();
        }

        doc.addPage();
        y = 36;

        // redraw header area for new page
        try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
        try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
        doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
        doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
        y += 56;
        doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
        y += 8;

        doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
        doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
        y += 28;

        // render table header on new page and reset trackers
        tableStartY = y;
        doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Sr.", colSrX + 4, y + 3);
        doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
        doc.text("Qty", colQtyX + 4, y + 3);
        doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
        doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
        y += headerHeight;
        doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

        filledHeight = 0;
      }

      // draw row text (centered vertically within row)
      const descH2 = doc.heightOfString(it.description || "", { width: colServiceW - 8 });
      const qtyH2 = doc.heightOfString(String(it.qty || ""), { width: colQtyW - 8 });
      const rateH2 = doc.heightOfString(formatMoney(it.rate || 0), { width: colRateW - 8 });
      const amtH2 = doc.heightOfString(formatMoney(it.amount || 0), { width: colSubW - 8 });
      const actualRowH = Math.max(minRowH, Math.max(descH2, qtyH2, rateH2, amtH2) + 8);

      const rowTop = y;
      const descY = rowTop + (actualRowH - descH2) / 2;
      const qtyY = rowTop + (actualRowH - qtyH2) / 2;
      const rateY = rowTop + (actualRowH - rateH2) / 2;
      const amtY = rowTop + (actualRowH - amtH2) / 2;

      doc.text(String(i + 1), colSrX + 4, rowTop + 4);
      doc.text(it.description || "", colServiceX + 4, descY, { width: colServiceW - 8 });
      doc.text(String(it.qty != null && it.qty !== "" ? it.qty : ""), colQtyX + 4, qtyY, { width: colQtyW - 8, align: "left" });
      doc.text(formatMoney(it.rate || 0), colRateX + 4, rateY, { width: colRateW - 8, align: "right" });
      doc.text(formatMoney(it.amount || 0), colSubX + 4, amtY, { width: colSubW - 8, align: "right" });

      y += actualRowH;
      filledHeight += actualRowH;
    }

    // ensure minimum visual height of the items block (adds filler rows if necessary)
    let totalTableHeight = filledHeight;
    if (totalTableHeight < minTableHeight) {
      let needed = minTableHeight - totalTableHeight;
      const fillerRows = Math.ceil(needed / minRowH);
      for (let fr = 0; fr < fillerRows; fr++) {
        if (y + minRowH > doc.page.height - bottomSafety) {
          // finalize previous page table visuals
          drawVerticalsForItemsBlock(tableStartY, headerHeight + totalTableHeight);
          doc.rect(tableLeft, tableStartY, usableWidth, headerHeight + totalTableHeight).stroke();

          doc.addPage();
          y = 36;

          // header context on new page
          try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
          try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
          doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
          doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
          y += 56;
          doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
          y += 8;

          doc.fontSize(10).font("Helvetica-Bold").rect(36, y, usableWidth, 18).stroke();
          doc.text("FULL PAYMENT RECEIPT", 36, y + 4, { width: usableWidth, align: "center" });
          y += 28;

          // render header again, reset trackers
          tableStartY = y;
          doc.save().rect(tableLeft, y, usableWidth, headerHeight).fill("#F3F3F3").restore();
          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Sr.", colSrX + 4, y + 3);
          doc.text("Description of Items / Services", colServiceX + 4, y + 3, { width: colServiceW - 8 });
          doc.text("Qty", colQtyX + 4, y + 3);
          doc.text("Rate", colRateX + 4, y + 3, { width: colRateW - 8, align: "right" });
          doc.text("Amount", colSubX + 4, y + 3, { width: colSubW - 8, align: "right" });
          y += headerHeight;
          doc.moveTo(tableLeft, y).lineTo(tableRightX, y).stroke();

          totalTableHeight = 0;
        }

        // filler row
        y += minRowH;
        totalTableHeight += minRowH;
      }
      filledHeight = totalTableHeight;
    }

    // draw vertical separators spanning header + content
    const visualTableHeight = headerHeight + filledHeight;
    drawVerticalsForItemsBlock(tableStartY, visualTableHeight);

    // draw outer border around the table block (header + content)
    doc.rect(tableLeft, tableStartY, usableWidth, visualTableHeight).stroke();

    // ===== Place Total Due box IMMEDIATELY AFTER TABLE (no gap) =====
    const boxWidth = 180;
    const boxHeight = 36; // height of totals box
    const boxX = tableRightX - boxWidth;
    // place top of box exactly at bottom edge of table (no gap)
    let boxY = tableStartY + visualTableHeight;

    // If the box doesn't fit on the current page, add a new page and place it near top (we keep payment details intact)
    if (boxY + boxHeight + 60 > doc.page.height) {
      doc.addPage();
      // redraw small header area for context
      try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, 36, { width: 40, height: 40 }); } catch (e) {}
      try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, 36, { width: 40, height: 40 }); } catch (e) {}
      doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, 42, { align: "center", width: pageWidth });
      // place the box near top of new page (just below header area)
      boxY = 90;
    }

    // draw the totals box attached to table bottom (or on new page if overflow)
    doc.rect(boxX, boxY, boxWidth, boxHeight).stroke();
    doc.font("Helvetica-Bold").fontSize(9).text("Total Due", boxX + 6, boxY + 6);
    doc.font("Helvetica").fontSize(9).text(`Rs ${formatMoney(total)}`, boxX, boxY + 6, { width: boxWidth - 8, align: "right" });

    // set y to continue AFTER the box (if box was on same page below table, this keeps payment details after box)
    if (boxY === tableStartY + visualTableHeight) {
      // box placed immediately after table on same page
      y = boxY + boxHeight + 8;
    } else {
      // box placed on new page near top, set y accordingly to place payment details below
      y = boxY + boxHeight + 12;
    }

    // ---------- PAYMENT DETAILS (chronological) — now WITHOUT horizontal lines, vertical separators only, dynamic heights ----------
    const pTableLeft = 36;
    const pColDateW = 60; // date/time area
    const pColRecW = 120;
    const pColModeW = 60;
    const pColBankW = 80;
    const pColRefW = 110;
    const pColAmtW = usableWidth - (pColDateW + pColRecW + pColModeW + pColBankW + pColRefW);

    const pColDateX = pTableLeft;
    const pColRecX = pColDateX + pColDateW;
    const pColModeX = pColRecX + pColRecW;
    const pColBankX = pColModeX + pColModeW;
    const pColRefX = pColBankX + pColBankW;
    const pColAmtX = pColRefX + pColRefW;
    const pTableRightX = pTableLeft + usableWidth;

    const pHeaderH = 18;
    const pMinRowH = 16;
    const pMinTableH = 120; // ensure payments table has a tidy minimum height
    const pBottomSafety = 120;

    function drawVerticalsForPaymentsBlock(yTop, height) {
      const xs = [pColDateX, pColRecX, pColModeX, pColBankX, pColRefX, pColAmtX, pTableRightX];
      xs.forEach((x) => {
        doc.moveTo(x, yTop).lineTo(x, yTop + height).stroke();
      });
    }

    // payments table header placement
    let pTableStartY = y;
    doc.save().rect(pTableLeft, y, usableWidth, pHeaderH).fill("#F3F3F3").restore();
    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
    doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
    doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
    doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
    doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
    doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });

    y += pHeaderH;
    // single horizontal separator UNDER header (like items)
    doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

    // render payments rows dynamically WITHOUT horizontal borders, track filled height
    doc.font("Helvetica").fontSize(9);
    let pFilledHeight = 0;
    for (let i = 0; i < payments.length; i++) {
      const p = payments[i];

      const dateTextP = formatDateOnly(p.paymentDateTime || p.paymentDate || `${p.paymentDate || ""}`);
      const receiptText = p.receiptNo || p.id || "";
      let modeText = p.mode || "-";
      if ((modeText === "BankTransfer" || (modeText && modeText.toLowerCase().includes("bank"))) && p.transferType) {
        modeText = `Bank (${p.transferType})`;
      }
      const bankText = p.bankName || "-";
      const refText = p.referenceNo || "-";
      const amtText = formatMoney(p.amount || 0);

      // compute heights for each cell
      const dH = doc.heightOfString(dateTextP, { width: pColDateW - 8 });
      const rH = doc.heightOfString(receiptText, { width: pColRecW - 8 });
      const mH = doc.heightOfString(modeText, { width: pColModeW - 8 });
      const bH = doc.heightOfString(bankText, { width: pColBankW - 8 });
      const refH = doc.heightOfString(refText, { width: pColRefW - 8 });
      const aH = doc.heightOfString(amtText, { width: pColAmtW - 8 });

      const maxH = Math.max(dH, rH, mH, bH, refH, aH);
      const thisRowH = Math.max(pMinRowH, maxH + 8);

      // page-break check before placing row
      if (y + thisRowH > doc.page.height - pBottomSafety) {
        // finalize payments block on this page (draw verticals + outer border)
        const paymentsVisualHeightSoFar = pHeaderH + pFilledHeight;
        if (paymentsVisualHeightSoFar > 0) {
          drawVerticalsForPaymentsBlock(pTableStartY, paymentsVisualHeightSoFar);
          doc.rect(pTableLeft, pTableStartY, usableWidth, paymentsVisualHeightSoFar).stroke();
        }

        doc.addPage();
        y = 36;

        // redraw header area/context
        try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
        try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
        doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
        doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
        y += 56;
        doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
        y += 8;

        // if payments header should appear again at top of new page
        pTableStartY = y;
        doc.save().rect(pTableLeft, y, usableWidth, pHeaderH).fill("#F3F3F3").restore();
        doc.font("Helvetica-Bold").fontSize(9);
        doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
        doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
        doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
        doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
        doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
        doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
        y += pHeaderH;
        doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

        pFilledHeight = 0;
      }

      // draw cell text (no horizontal border)
      const rowTop = y;
      const dateY = rowTop + (thisRowH - dH) / 2;
      const recY = rowTop + (thisRowH - rH) / 2;
      const modeY = rowTop + (thisRowH - mH) / 2;
      const bankY = rowTop + (thisRowH - bH) / 2;
      const refY = rowTop + (thisRowH - refH) / 2;
      const amtY = rowTop + (thisRowH - aH) / 2;

      doc.text(dateTextP, pColDateX + 4, dateY, { width: pColDateW - 8 });
      doc.text(receiptText, pColRecX + 4, recY, { width: pColRecW - 8 });
      doc.text(modeText, pColModeX + 4, modeY, { width: pColModeW - 8 });
      doc.text(bankText, pColBankX + 4, bankY, { width: pColBankW - 8 });
      doc.text(refText, pColRefX + 4, refY, { width: pColRefW - 8 });
      doc.text(amtText, pColAmtX + 4, amtY, { width: pColAmtW - 8, align: "right" });

      // advance
      y += thisRowH;
      pFilledHeight += thisRowH;
    }

    // After rendering all payments rows on current page, ensure payments block has a minimum visual height
    if (pFilledHeight < pMinTableH) {
      const need = pMinTableH - pFilledHeight;
      const fillerCount = Math.ceil(need / pMinRowH);
      for (let f = 0; f < fillerCount; f++) {
        if (y + pMinRowH > doc.page.height - pBottomSafety) {
          // finalize visuals for this page
          const paymentsVisualHeightSoFar = pHeaderH + pFilledHeight;
          if (paymentsVisualHeightSoFar > 0) {
            drawVerticalsForPaymentsBlock(pTableStartY, paymentsVisualHeightSoFar);
            doc.rect(pTableLeft, pTableStartY, usableWidth, paymentsVisualHeightSoFar).stroke();
          }
          doc.addPage();
          y = 36;

          // redraw header context and payments header
          try { if (fs.existsSync(logoLeftPath)) doc.image(logoLeftPath, 36, y, { width: 40, height: 40 }); } catch (e) {}
          try { if (fs.existsSync(logoRightPath)) doc.image(logoRightPath, pageWidth - 36 - 40, y, { width: 40, height: 40 }); } catch (e) {}
          doc.fontSize(14).font("Helvetica-Bold").text(clinicName || "", 0, y + 6, { align: "center", width: pageWidth });
          doc.fontSize(9).font("Helvetica").text(clinicAddress || "", 0, y + 28, { align: "center", width: pageWidth });
          y += 56;
          doc.moveTo(36, y).lineTo(pageWidth - 36, y).stroke();
          y += 8;

          // render header again
          pTableStartY = y;
          doc.save().rect(pTableLeft, y, usableWidth, pHeaderH).fill("#F3F3F3").restore();
          doc.font("Helvetica-Bold").fontSize(9);
          doc.text("Date", pColDateX + 4, y + 4, { width: pColDateW - 8 });
          doc.text("Receipt No.", pColRecX + 4, y + 4, { width: pColRecW - 8 });
          doc.text("Mode", pColModeX + 4, y + 4, { width: pColModeW - 8 });
          doc.text("Bank Name", pColBankX + 4, y + 4, { width: pColBankW - 8 });
          doc.text("Reference", pColRefX + 4, y + 4, { width: pColRefW - 8 });
          doc.text("Amount", pColAmtX + 4, y + 4, { width: pColAmtW - 8, align: "right" });
          y += pHeaderH;
          doc.moveTo(pTableLeft, y).lineTo(pTableRightX, y).stroke();

          pFilledHeight = 0;
        }

        // filler row (visual)
        y += pMinRowH;
        pFilledHeight += pMinRowH;
      }
    }

    // finalize payments block: draw vertical separators and outer border for the block on the current page
    if (pFilledHeight > 0) {
      const paymentsVisualHeight = pHeaderH + pFilledHeight;
      drawVerticalsForPaymentsBlock(pTableStartY, paymentsVisualHeight);
      doc.rect(pTableLeft, pTableStartY, usableWidth, paymentsVisualHeight).stroke();
      // move y pointer to below payments block (ensure some gap)
      y = pTableStartY + paymentsVisualHeight + 12;
    }

    // ---------- FOOTER TOTALS BOX (kept as additional summary at left) ----------
    const boxWidth2 = 260;
    const boxX2 = 36;
    const boxY2 = y;
    const lineH = 14;
    const rowsCount = 5;
    const boxHeight2 = rowsCount * lineH + 8;

    // If not enough space, add new page
    if (boxY2 + boxHeight2 + 60 > doc.page.height) {
      doc.addPage();
      y = 36;
    }

    doc.rect(boxX2, boxY2, boxWidth2, boxHeight2).stroke();
    let by = boxY2 + 6;
    doc.font("Helvetica").fontSize(9);

    const addRow = (label, value) => {
      doc.text(label, boxX2 + 6, by);
      doc.text(value, boxX2 + 6, by, { width: boxWidth2 - 12, align: "right" });
      by += lineH;
    };

    addRow("Total", `Rs ${formatMoney(total)}`);
    addRow("Total Paid (gross)", `Rs ${formatMoney(totalPaidGross)}`);
    addRow("Net Paid (after refunds)", `Rs ${formatMoney(netPaid)}`);
    addRow("Total Refunded", `Rs ${formatMoney(totalRefunded)}`);
    addRow("Balance", `Rs ${formatMoney(balance)}`);

    y = boxY2 + boxHeight2 + 20;

    // footer note + signatures (same as invoice)
    doc.fontSize(8).text("* This receipt is generated by the clinic. Disputes if any are subject to local jurisdiction.", 36, y, { width: usableWidth });
    const sigY = y + 28;
    const sigWidth = 160;
    doc.moveTo(36, sigY).lineTo(36 + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
    doc.fontSize(8).text(patientRepresentative || "", 36, sigY + 4, { width: sigWidth, align: "center" });
    const rightSigX = pageWidth - 36 - sigWidth;
    doc.moveTo(rightSigX, sigY).lineTo(rightSigX + sigWidth, sigY).dash(1, { space: 2 }).stroke().undash();
    doc.fontSize(8).text(clinicRepresentative || "", rightSigX, sigY + 4, { width: sigWidth, align: "center" });

    doc.end();
  } catch (err) {
    console.error("full-payment-pdf error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to generate full payment PDF" });
    } else {
      try { res.end(); } catch (e) {}
    }
  }
});


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

// ---------- START SERVER ----------
app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
