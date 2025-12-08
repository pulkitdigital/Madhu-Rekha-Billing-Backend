// sheetIntregation.js
import "dotenv/config";
import fetch from "node-fetch";

const SHEETS_WEBHOOK_URL = process.env.SHEETS_WEBHOOK_URL; // Apps Script web app URL
const SHEETS_WEBHOOK_SECRET = process.env.SHEETS_WEBHOOK_SECRET; // shared secret

if (!SHEETS_WEBHOOK_URL) {
  console.warn("[Sheets] SHEETS_WEBHOOK_URL not set – sheet sync will be skipped.");
}
if (!SHEETS_WEBHOOK_SECRET) {
  console.warn("[Sheets] SHEETS_WEBHOOK_SECRET not set – sheet sync will be skipped.");
}

async function postToSheets(type, row) {
  if (!SHEETS_WEBHOOK_URL || !SHEETS_WEBHOOK_SECRET) return;

  try {
    const res = await fetch(SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        secret: SHEETS_WEBHOOK_SECRET,
        type,     // "bill" | "item" | "payment" | "refund"
        row,      // flat object with fields
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      console.error(`[Sheets] Sync failed (${res.status}):`, text);
    }
  } catch (err) {
    console.error("[Sheets] Sync error:", err);
  }
}

// ---------- PUBLIC HELPERS ----------

// 1) BILL ROW
export async function syncBillToSheet(bill) {
  // bill is your bill object after creation
  const row = {
    timestamp: new Date().toISOString(),
    billId: bill.id || bill.invoiceNo,
    invoiceNo: bill.invoiceNo,
    patientName: bill.patientName || "",
    billDate: bill.date || "",
    subtotal: Number(bill.subtotal || 0),
    adjust: Number(bill.adjust || 0),
    total: Number(bill.total || 0),
    paid: Number(bill.paid || 0),
    refunded: Number(bill.refunded || 0),
    balance: Number(bill.balance || 0),
    status: bill.status || "",
    doctorReg1: bill.doctorReg1 || "",
    doctorReg2: bill.doctorReg2 || "",
    age: bill.age != null ? Number(bill.age) : "",
    address: bill.address || "",
  };

  await postToSheets("bill", row);
}

// 2) ITEMS FOR A BILL (one row per item)
export async function syncItemsToSheet(billId, invoiceNo, patientName, items = []) {
  for (const item of items) {
    const row = {
      timestamp: new Date().toISOString(),
      billId,
      invoiceNo,
      patientName: patientName || "",
      description: item.description || "",
      qty: Number(item.qty || 0),
      rate: Number(item.rate || 0),
      amount: Number(item.amount || 0),
    };
    await postToSheets("item", row);
  }
}

// 3) PAYMENT ROW
export async function syncPaymentToSheet(payment, bill) {
  const row = {
    timestamp: new Date().toISOString(),
    paymentId: payment.id || "",
    billId: payment.billId,
    invoiceNo: (bill && (bill.invoiceNo || bill.id)) || "",
    patientName: (bill && bill.patientName) || "",
    paymentDate: payment.paymentDate || "",
    paymentTime: payment.paymentTime || "",
    amount: Number(payment.amount || 0),
    mode: payment.mode || "",
    referenceNo: payment.referenceNo || "",
    drawnOn: payment.drawnOn || "",
    drawnAs: payment.drawnAs || "",
    receiptNo: payment.receiptNo || "",
  };

  await postToSheets("payment", row);
}

// 4) REFUND ROW
export async function syncRefundToSheet(refund, bill) {
  const row = {
    timestamp: new Date().toISOString(),
    refundId: refund.id || "",
    billId: refund.billId,
    invoiceNo: (bill && (bill.invoiceNo || bill.id)) || "",
    patientName: (bill && bill.patientName) || "",
    refundDate: refund.refundDate || "",
    refundTime: refund.refundTime || "",
    amount: Number(refund.amount || 0),
    mode: refund.mode || "",
    referenceNo: refund.referenceNo || "",
    drawnOn: refund.drawnOn || "",
    drawnAs: refund.drawnAs || "",
    refundReceiptNo: refund.refundReceiptNo || "",
  };

  await postToSheets("refund", row);
}
