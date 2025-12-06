// firebaseClient.js
import "dotenv/config"; // ensure .env is loaded even if server.js fails
import admin from "firebase-admin";

if (!admin.apps.length) {
  const serviceAccount = {
    // support both FIREBASE_TYPE and legacy "type"
    type: process.env.FIREBASE_TYPE || process.env.type || "service_account",

    // yaha tumhari main problem thi:
    project_id:
      process.env.FIREBASE_PROJECT_ID || process.env.project_id,

    private_key_id:
      process.env.FIREBASE_PRIVATE_KEY_ID || process.env.private_key_id,

    private_key: (
      process.env.FIREBASE_PRIVATE_KEY || process.env.private_key
    )
      ? (process.env.FIREBASE_PRIVATE_KEY || process.env.private_key).replace(
          /\\n/g,
          "\n"
        )
      : undefined,

    client_email:
      process.env.FIREBASE_CLIENT_EMAIL || process.env.client_email,
    client_id:
      process.env.FIREBASE_CLIENT_ID || process.env.client_id,
    auth_uri:
      process.env.FIREBASE_AUTH_URI || process.env.auth_uri,
    token_uri:
      process.env.FIREBASE_TOKEN_URI || process.env.token_uri,
    auth_provider_x509_cert_url:
      process.env.FIREBASE_AUTH_PROVIDER_X509_CERT_URL ||
      process.env.auth_provider_x509_cert_url,
    client_x509_cert_url:
      process.env.FIREBASE_CLIENT_X509_CERT_URL ||
      process.env.client_x509_cert_url,
  };

  console.log("Service account project_id =", serviceAccount.project_id);

  if (!serviceAccount.project_id) {
    throw new Error("No project_id found in env (FIREBASE_PROJECT_ID or project_id)");
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db = admin.firestore();

export { admin, db };
