// // sheetIntregation.js
// import "dotenv/config";
// import fetch from "node-fetch";

// const SHEETS_WEBHOOK_URL = process.env.SHEETS_WEBHOOK_URL; // Apps Script web app URL
// const SHEETS_WEBHOOK_SECRET = process.env.SHEETS_WEBHOOK_SECRET; // shared secret

// if (!SHEETS_WEBHOOK_URL) {
//   console.warn("[Sheets] SHEETS_WEBHOOK_URL not set – sheet sync will be skipped.");
// }
// if (!SHEETS_WEBHOOK_SECRET) {
//   console.warn("[Sheets] SHEETS_WEBHOOK_SECRET not set – sheet sync will be skipped.");
// }

// async function postToSheets(type, row) {
//   if (!SHEETS_WEBHOOK_URL || !SHEETS_WEBHOOK_SECRET) return;

//   try {
//     const res = await fetch(SHEETS_WEBHOOK_URL, {
//       method: "POST",
//       headers: { "Content-Type": "application/json" },
//       body: JSON.stringify({
//         secret: SHEETS_WEBHOOK_SECRET,
//         type,     // "bill" | "item" | "payment" | "refund"
//         row,      // flat object with fields
//       }),
//     });

//     if (!res.ok) {
//       const text = await res.text();
//       console.error(`[Sheets] Sync failed (${res.status}):`, text);
//     }
//   } catch (err) {
//     console.error("[Sheets] Sync error:", err);
//   }
// }

// // ---------- PUBLIC HELPERS ----------

// // 1) BILL ROW
// export async function syncBillToSheet(bill) {
//   // bill is your bill object after creation
//   const row = {
//     timestamp: new Date().toISOString(),
//     billId: bill.id || bill.invoiceNo,
//     invoiceNo: bill.invoiceNo,
//     patientName: bill.patientName || "",
//     billDate: bill.date || "",
//     subtotal: Number(bill.subtotal || 0),
//     adjust: Number(bill.adjust || 0),
//     total: Number(bill.total || 0),
//     paid: Number(bill.paid || 0),
//     refunded: Number(bill.refunded || 0),
//     balance: Number(bill.balance || 0),
//     status: bill.status || "",
//     doctorReg1: bill.doctorReg1 || "",
//     doctorReg2: bill.doctorReg2 || "",
//     age: bill.age != null ? Number(bill.age) : "",
//     address: bill.address || "",
//   };

//   await postToSheets("bill", row);
// }

// // 2) ITEMS FOR A BILL (one row per item)
// export async function syncItemsToSheet(billId, invoiceNo, patientName, items = []) {
//   for (const item of items) {
//     const row = {
//       timestamp: new Date().toISOString(),
//       billId,
//       invoiceNo,
//       patientName: patientName || "",
//       description: item.description || "",
//       qty: Number(item.qty || 0),
//       rate: Number(item.rate || 0),
//       amount: Number(item.amount || 0),
//     };
//     await postToSheets("item", row);
//   }
// }

// // 3) PAYMENT ROW
// export async function syncPaymentToSheet(payment, bill) {
//   const row = {
//     timestamp: new Date().toISOString(),
//     paymentId: payment.id || "",
//     billId: payment.billId,
//     invoiceNo: (bill && (bill.invoiceNo || bill.id)) || "",
//     patientName: (bill && bill.patientName) || "",
//     paymentDate: payment.paymentDate || "",
//     paymentTime: payment.paymentTime || "",
//     amount: Number(payment.amount || 0),
//     mode: payment.mode || "",
//     referenceNo: payment.referenceNo || "",
//     drawnOn: payment.drawnOn || "",
//     drawnAs: payment.drawnAs || "",
//     receiptNo: payment.receiptNo || "",
//   };

//   await postToSheets("payment", row);
// }

// // 4) REFUND ROW
// export async function syncRefundToSheet(refund, bill) {
//   const row = {
//     timestamp: new Date().toISOString(),
//     refundId: refund.id || "",
//     billId: refund.billId,
//     invoiceNo: (bill && (bill.invoiceNo || bill.id)) || "",
//     patientName: (bill && bill.patientName) || "",
//     refundDate: refund.refundDate || "",
//     refundTime: refund.refundTime || "",
//     amount: Number(refund.amount || 0),
//     mode: refund.mode || "",
//     referenceNo: refund.referenceNo || "",
//     drawnOn: refund.drawnOn || "",
//     drawnAs: refund.drawnAs || "",
//     refundReceiptNo: refund.refundReceiptNo || "",
//   };

//   await postToSheets("refund", row);
// }



// // sheetIntregation.js
// import "dotenv/config";
// import fetch from "node-fetch";

// const SHEETS_WEBHOOK_URL = process.env.SHEETS_WEBHOOK_URL; // Apps Script web app URL
// const SHEETS_WEBHOOK_SECRET = process.env.SHEETS_WEBHOOK_SECRET; // shared secret

// if (!SHEETS_WEBHOOK_URL) {
//   console.warn("[Sheets] SHEETS_WEBHOOK_URL not set – sheet sync will be skipped.");
// }
// if (!SHEETS_WEBHOOK_SECRET) {
//   console.warn("[Sheets] SHEETS_WEBHOOK_SECRET not set – sheet sync will be skipped.");
// }

// async function postToSheet(type, row) {
//   try {
//     await fetch(SHEETS_WEBHOOK_URL, {
//       method: "POST",
//       headers: {
//         "Content-Type": "application/json",
//       },
//       body: JSON.stringify({
//         secret: SHEETS_WEBHOOK_SECRET,
//         type,
//         row,
//       }),
//     });
//   } catch (err) {
//     console.error("Error posting to sheet", type, err);
//   }
// }

// // ---------- BILLS ----------
// // server.js se object roughly aisa aata hai:
// // {
// //   id, invoiceNo, patientName, address, age, date,
// //   subtotal, adjust, total, paid, refunded, balance, status, sex (optional)
// // }
// // Yaha hum doctorReg1/2 ya doctor name ko IGNORE kar rahe hain.
// export async function syncBillToSheet(bill) {
//   const row = {
//     timestamp: new Date().toISOString(),
//     billId: bill.id,
//     invoiceNo: bill.invoiceNo,
//     patientName: bill.patientName || "",
//     billDate: bill.date || "",
//     subtotal: Number(bill.subtotal || 0),
//     adjust: Number(bill.adjust || 0),
//     total: Number(bill.total || 0),
//     paid: Number(bill.paid || 0),
//     refunded: Number(bill.refunded || 0),
//     balance: Number(bill.balance || 0),
//     status: bill.status || "",
//     age: bill.age != null ? bill.age : "",
//     address: bill.address || "",
//     sex: bill.sex || "",
//   };

//   await postToSheet("bill", row);
// }

// // ---------- ITEMS ----------
// // server.js call:
// // syncItemsToSheet(billId, invoiceNo, patientName, itemsData);
// export async function syncItemsToSheet(billId, invoiceNo, patientName, items) {
//   const base = {
//     billId,
//     invoiceNo,
//     patientName: patientName || "",
//   };

//   for (const item of items || []) {
//     const row = {
//       timestamp: new Date().toISOString(),
//       ...base,
//       description: item.description || "",
//       qty: Number(item.qty || 0),
//       rate: Number(item.rate || 0),
//       amount: Number(item.amount || 0),
//     };

//     await postToSheet("item", row);
//   }
// }

// // ---------- PAYMENTS ----------
// /**
//  * server.js se call:
//  * syncPaymentToSheet(
//  *   { id: paymentRef.id, ...paymentDoc },
//  *   { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
//  * );
//  *
//  * paymentDoc fields:
//  *  billId, amount, mode, referenceNo, drawnOn, drawnAs,
//  *  chequeDate, chequeNumber, bankName,
//  *  transferType, transferDate,
//  *  upiName, upiId, upiDate,
//  *  paymentDate, paymentTime, paymentDateTime, receiptNo
//  */
// export async function syncPaymentToSheet(payment, bill) {
//   const row = {
//     timestamp: new Date().toISOString(),
//     paymentId: payment.id,
//     billId: bill.id || payment.billId,
//     invoiceNo: bill.invoiceNo || "",
//     patientName: bill.patientName || "",
//     paymentDate: payment.paymentDate || "",
//     paymentTime: payment.paymentTime || "",
//     amount: Number(payment.amount || 0),
//     mode: payment.mode || "",
//     referenceNo: payment.referenceNo || "",
//     drawnOn: payment.drawnOn || "",
//     drawnAs: payment.drawnAs || "",
//     receiptNo: payment.receiptNo || "",
//     chequeDate: payment.chequeDate || "",
//     chequeNumber: payment.chequeNumber || "",
//     bankName: payment.bankName || "",
//     transferType: payment.transferType || "",
//     transferDate: payment.transferDate || "",
//     upiName: payment.upiName || "",
//     upiId: payment.upiId || "",
//     upiDate: payment.upiDate || "",
//   };

//   await postToSheet("payment", row);
// }

// // ---------- REFUNDS ----------
// /**
//  * server.js se call (thoda sa change kiya hua version):
//  * syncRefundToSheet(
//  *   { id: refundRef.id, ...refundDoc, netPaidAfterThis: effectivePaid, balanceAfterThis: newBalance },
//  *   { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
//  * );
//  */
// export async function syncRefundToSheet(refund, bill) {
//   const row = {
//     timestamp: new Date().toISOString(),
//     refundId: refund.id,
//     billId: bill.id || refund.billId,
//     invoiceNo: bill.invoiceNo || "",
//     patientName: bill.patientName || "",
//     refundDate: refund.refundDate || "",
//     refundTime: refund.refundTime || "",
//     amount: Number(refund.amount || 0),
//     mode: refund.mode || "",
//     referenceNo: refund.referenceNo || "",
//     drawnOn: refund.drawnOn || "",
//     drawnAs: refund.drawnAs || "",
//     refundReceiptNo: refund.refundReceiptNo || "",
//     chequeDate: refund.chequeDate || "",
//     chequeNumber: refund.chequeNumber || "",
//     bankName: refund.bankName || "",
//     transferType: refund.transferType || "",
//     transferDate: refund.transferDate || "",
//     upiName: refund.upiName || "",
//     upiId: refund.upiId || "",
//     upiDate: refund.upiDate || "",
//     netPaidAfterThis:
//       typeof refund.netPaidAfterThis === "number"
//         ? refund.netPaidAfterThis
//         : "",
//     balanceAfterThis:
//       typeof refund.balanceAfterThis === "number"
//         ? refund.balanceAfterThis
//         : "",
//   };

//   await postToSheet("refund", row);
// }














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

/**
 * Generic POST helper used for add/update rows.
 * This intentionally swallows errors (doesn't throw) so sheet failures
 * don't break the main DB workflow; they are logged instead.
 */
async function postToSheet(type, row) {
  if (!SHEETS_WEBHOOK_URL || !SHEETS_WEBHOOK_SECRET) return;
  try {
    await fetch(SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        secret: SHEETS_WEBHOOK_SECRET,
        type,
        row,
      }),
    });
  } catch (err) {
    console.error("Error posting to sheet", type, err);
  }
}

/**
 * Generic POST helper for administrative actions (delete).
 * Returns an object { ok: boolean, status, body } or throws if fetch fails to run.
 * Caller should catch/log as needed.
 */
async function postToSheetAction(type, payload = {}) {
  if (!SHEETS_WEBHOOK_URL || !SHEETS_WEBHOOK_SECRET) {
    const msg = "[Sheets] webhook not configured";
    console.warn(msg);
    return { ok: false, error: msg };
  }

  try {
    const resp = await fetch(SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        secret: SHEETS_WEBHOOK_SECRET,
        type,
        payload,
      }),
    });

    const text = await resp.text().catch(() => null);
    let body = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch (e) {
      body = text;
    }

    return { ok: resp.ok, status: resp.status, body };
  } catch (err) {
    console.error("postToSheetAction error:", err);
    return { ok: false, error: String(err) };
  }
}

// ---------- BILLS ----------
/*
 server.js se object roughly aisa aata hai:
 {
   id, invoiceNo, patientName, address, age, date,
   subtotal, adjust, total, paid, refunded, balance, status, sex (optional)
 }
 Yaha hum doctorReg1/2 ya doctor name ko IGNORE kar rahe hain.
*/
export async function syncBillToSheet(bill) {
  const row = {
    timestamp: new Date().toISOString(),
    billId: bill.id,
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
    age: bill.age != null ? bill.age : "",
    address: bill.address || "",
    sex: bill.sex || "",
  };

  await postToSheet("bill", row);
}

// ---------- ITEMS ----------
/*
 server.js call:
 syncItemsToSheet(billId, invoiceNo, patientName, itemsData);
*/
export async function syncItemsToSheet(billId, invoiceNo, patientName, items) {
  const base = {
    billId,
    invoiceNo,
    patientName: patientName || "",
  };

  for (const item of items || []) {
    const row = {
      timestamp: new Date().toISOString(),
      ...base,
      description: item.description || "",
      qty: Number(item.qty || 0),
      rate: Number(item.rate || 0),
      amount: Number(item.amount || 0),
    };

    await postToSheet("item", row);
  }
}

// ---------- PAYMENTS ----------
/**
 * server.js se call:
 * syncPaymentToSheet(
 *   { id: paymentRef.id, ...paymentDoc },
 *   { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
 * );
 *
 * paymentDoc fields:
 *  billId, amount, mode, referenceNo, drawnOn, drawnAs,
 *  chequeDate, chequeNumber, bankName,
 *  transferType, transferDate,
 *  upiName, upiId, upiDate,
 *  paymentDate, paymentTime, paymentDateTime, receiptNo
 */
export async function syncPaymentToSheet(payment, bill) {
  const row = {
    timestamp: new Date().toISOString(),
    paymentId: payment.id,
    billId: bill.id || payment.billId,
    invoiceNo: bill.invoiceNo || "",
    patientName: bill.patientName || "",
    paymentDate: payment.paymentDate || "",
    paymentTime: payment.paymentTime || "",
    amount: Number(payment.amount || 0),
    mode: payment.mode || "",
    referenceNo: payment.referenceNo || "",
    drawnOn: payment.drawnOn || "",
    drawnAs: payment.drawnAs || "",
    receiptNo: payment.receiptNo || "",
    chequeDate: payment.chequeDate || "",
    chequeNumber: payment.chequeNumber || "",
    bankName: payment.bankName || "",
    transferType: payment.transferType || "",
    transferDate: payment.transferDate || "",
    upiName: payment.upiName || "",
    upiId: payment.upiId || "",
    upiDate: payment.upiDate || "",
  };

  await postToSheet("payment", row);
}

// ---------- REFUNDS ----------
/**
 * server.js se call:
 * syncRefundToSheet(
 *   { id: refundRef.id, ...refundDoc, netPaidAfterThis: effectivePaid, balanceAfterThis: newBalance },
 *   { id: billId, invoiceNo: bill.invoiceNo, patientName: bill.patientName }
 * );
 */
export async function syncRefundToSheet(refund, bill) {
  const row = {
    timestamp: new Date().toISOString(),
    refundId: refund.id,
    billId: bill.id || refund.billId,
    invoiceNo: bill.invoiceNo || "",
    patientName: bill.patientName || "",
    refundDate: refund.refundDate || "",
    refundTime: refund.refundTime || "",
    amount: Number(refund.amount || 0),
    mode: refund.mode || "",
    referenceNo: refund.referenceNo || "",
    drawnOn: refund.drawnOn || "",
    drawnAs: refund.drawnAs || "",
    refundReceiptNo: refund.refundReceiptNo || "",
    chequeDate: refund.chequeDate || "",
    chequeNumber: refund.chequeNumber || "",
    bankName: refund.bankName || "",
    transferType: refund.transferType || "",
    transferDate: refund.transferDate || "",
    upiName: refund.upiName || "",
    upiId: refund.upiId || "",
    upiDate: refund.upiDate || "",
    netPaidAfterThis:
      typeof refund.netPaidAfterThis === "number"
        ? refund.netPaidAfterThis
        : "",
    balanceAfterThis:
      typeof refund.balanceAfterThis === "number"
        ? refund.balanceAfterThis
        : "",
  };

  await postToSheet("refund", row);
}

/* ---------------------------
   DELETE helpers (sheet side)
   These call your Apps Script webhook with an action type that your script must handle:
     - delete_bill           { invoiceNo }
     - delete_items          { billId }
     - delete_payments       { billId }
     - delete_refunds        { billId }
   The Apps Script should implement the logic to find & remove matching rows.
   --------------------------- */

/**
 * Delete bill row(s) in sheet by invoiceNo.
 * Returns { ok, status, body } which contains the webhook response (if any).
 *
 * opts: optional object you can pass to provide extra fields to the webhook,
 * for example { sheetName: 'Bills' } if your AppsScript expects that.
 */
export async function syncDeleteBillFromSheet(invoiceNo, opts = {}) {
  if (!invoiceNo) {
    const msg = "invoiceNo required for syncDeleteBillFromSheet";
    console.warn(msg);
    return { ok: false, error: msg };
  }

  try {
    const payload = { invoiceNo: String(invoiceNo), ...(opts || {}) };
    const result = await postToSheetAction("delete_bill", payload);
    if (!result.ok) {
      console.warn("syncDeleteBillFromSheet webhook returned not-ok", result);
    }
    return result;
  } catch (err) {
    console.error("syncDeleteBillFromSheet error:", err);
    return { ok: false, error: String(err) };
  }
}

/**
 * Delete item rows in sheet by billId.
 * Returns webhook response object.
 */
export async function syncDeleteItemsFromSheet(billId, opts = {}) {
  if (!billId) {
    const msg = "billId required for syncDeleteItemsFromSheet";
    console.warn(msg);
    return { ok: false, error: msg };
  }

  try {
    const payload = { billId: String(billId), ...(opts || {}) };
    const result = await postToSheetAction("delete_items", payload);
    if (!result.ok) {
      console.warn("syncDeleteItemsFromSheet webhook returned not-ok", result);
    }
    return result;
  } catch (err) {
    console.error("syncDeleteItemsFromSheet error:", err);
    return { ok: false, error: String(err) };
  }
}

/**
 * Delete payment rows in sheet by billId.
 * Returns webhook response object.
 */
export async function syncDeletePaymentsFromSheet(billId, opts = {}) {
  if (!billId) {
    const msg = "billId required for syncDeletePaymentsFromSheet";
    console.warn(msg);
    return { ok: false, error: msg };
  }

  try {
    const payload = { billId: String(billId), ...(opts || {}) };
    const result = await postToSheetAction("delete_payments", payload);
    if (!result.ok) {
      console.warn("syncDeletePaymentsFromSheet webhook returned not-ok", result);
    }
    return result;
  } catch (err) {
    console.error("syncDeletePaymentsFromSheet error:", err);
    return { ok: false, error: String(err) };
  }
}

/**
 * Delete refund rows in sheet by billId.
 * Returns webhook response object.
 */
export async function syncDeleteRefundsFromSheet(billId, opts = {}) {
  if (!billId) {
    const msg = "billId required for syncDeleteRefundsFromSheet";
    console.warn(msg);
    return { ok: false, error: msg };
  }

  try {
    const payload = { billId: String(billId), ...(opts || {}) };
    const result = await postToSheetAction("delete_refunds", payload);
    if (!result.ok) {
      console.warn("syncDeleteRefundsFromSheet webhook returned not-ok", result);
    }
    return result;
  } catch (err) {
    console.error("syncDeleteRefundsFromSheet error:", err);
    return { ok: false, error: String(err) };
  }
}

/* Export summary (optional convenience) */
export default {
  syncBillToSheet,
  syncItemsToSheet,
  syncPaymentToSheet,
  syncRefundToSheet,
  syncDeleteBillFromSheet,
  syncDeleteItemsFromSheet,
  syncDeletePaymentsFromSheet,
  syncDeleteRefundsFromSheet,
};
