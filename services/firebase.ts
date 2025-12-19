import firebase from "firebase/compat/app";
import "firebase/compat/auth";
import "firebase/compat/firestore";
import "firebase/compat/storage";

const firebaseConfig = {
  apiKey: "AIzaSyCijGUuiqUjDhBu1H0IClZdW-SUrTc-OEk",
  authDomain: "mushroom1-1db55.firebaseapp.com",
  projectId: "mushroom1-1db55",
  storageBucket: "mushroom1-1db55.firebasestorage.app",
  messagingSenderId: "381076705990",
  appId: "1:381076705990:web:b35189ae6c51dd6cf77dc7"
};

// Initialize Firebase
const app = !firebase.apps.length ? firebase.initializeApp(firebaseConfig) : firebase.app();
export const auth = app.auth();
export const db = app.firestore();
export const storage = app.storage();

export default app;