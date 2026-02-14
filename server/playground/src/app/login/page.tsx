"use client";

import { useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "../../context/AuthContext";
import { auth } from "../../lib/firebase";
import { GoogleAuthProvider, EmailAuthProvider } from "firebase/auth";
import "firebaseui/dist/firebaseui.css";

export default function LoginPage() {
    const { user, loading } = useAuth();
    const router = useRouter();
    const uiContainerRef = useRef<HTMLDivElement>(null);
    const uiInstanceRef = useRef<any>(null);

    useEffect(() => {
        if (user) {
            router.push("/");
        }
    }, [user, router]);

    useEffect(() => {
        // Ensure code runs only in client
        if (typeof window === "undefined") return;

        const loadFirebaseUI = async () => {
            try {
                const firebaseuiModule = await import("firebaseui");
                // Handle default export if it exists (common in Next.js CJS imports)
                const firebaseui = firebaseuiModule.default || firebaseuiModule;

                console.log("FirebaseUI loaded", firebaseui);

                // Initialize FirebaseUI instance or get existing one
                let ui = firebaseui.auth.AuthUI.getInstance();
                if (!ui) {
                    // Check if 'auth' is valid
                    if (!auth) {
                        console.error("Firebase Auth instance is missing!");
                        return;
                    }
                    ui = new firebaseui.auth.AuthUI(auth);
                    console.log("Created new AuthUI instance");
                } else {
                    console.log("Using existing AuthUI instance");
                }
                uiInstanceRef.current = ui;

                const uiConfig = {
                    signInFlow: "popup",
                    signInSuccessUrl: "/",
                    signInOptions: [
                        {
                            provider: GoogleAuthProvider.PROVIDER_ID,
                            customParameters: { prompt: 'select_account' }
                        },
                        EmailAuthProvider.PROVIDER_ID,
                    ],
                    callbacks: {
                        signInSuccessWithAuthResult: () => false,
                        uiShown: () => {
                            console.log("FirebaseUI UI shown callback triggered");
                        }
                    },
                };

                // Use ID selector directly
                const containerId = '#firebaseui-auth-container';
                if (document.querySelector(containerId)) {
                    ui.reset();
                    // setTimeout to ensure next tick rendering
                    setTimeout(() => {
                        ui.start(containerId, uiConfig);
                        console.log("AuthUI started on", containerId);
                    }, 0);
                } else {
                    console.error("FirebaseUI Container not found in DOM");
                }
            } catch (error) {
                console.error("Failed to load/start FirebaseUI:", error);
            }
        };

        loadFirebaseUI();

        return () => {
            const ui = uiInstanceRef.current;
            if (ui) {
                ui.reset();
            }
        };
    }, []);

    if (loading) return <p className="p-10">Loading...</p>;

    return (
        <div className="flex min-h-screen flex-col items-center justify-center p-24 bg-gray-50">
            <div className="bg-white p-8 rounded-xl shadow-lg w-full max-w-md">
                <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">Welcome</h1>
                <p className="text-center text-gray-600 mb-8">Sign in or register to continue</p>
                <div ref={uiContainerRef} id="firebaseui-auth-container" className="min-h-[200px] w-full border border-gray-100 rounded-md p-4" />
            </div>
        </div>
    );
}
