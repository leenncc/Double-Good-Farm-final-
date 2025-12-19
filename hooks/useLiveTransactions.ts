import { useState, useEffect } from "react";
import { db } from "../services/firebase";
import firebase from "firebase/compat/app";

export const useLiveTransactions = () => {
  const [transactions, setTransactions] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Reference the 'mushroom_transactions' collection in the database
    const collectionRef = db.collection("mushroom_transactions");
    
    // Sort by newest first
    const q = collectionRef.orderBy("createdAt", "desc");

    // Open a live connection (listener)
    const unsubscribe = q.onSnapshot((snapshot) => {
      const liveData = snapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      }));
      setTransactions(liveData);
      setLoading(false);
    });

    // Cleanup when leaving the screen
    return () => unsubscribe();
  }, []);

  // Function to add new data
  const addTransaction = async (data: any) => {
    try {
      await db.collection("mushroom_transactions").add({
        ...data,
        createdAt: firebase.firestore.FieldValue.serverTimestamp()
      });
    } catch (error) {
      console.error("Error adding document: ", error);
    }
  };

  return { transactions, loading, addTransaction };
};