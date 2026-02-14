import { auth } from "./firebase";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

/**
 * Helper to make authenticated requests to the backend.
 * Automatically attaches the Firebase ID token to the Authorization header.
 */
export async function apiRequest(endpoint: string, options: RequestInit = {}) {
    const user = auth.currentUser;
    const headers = new Headers(options.headers || {});

    if (user) {
        const token = await user.getIdToken();
        headers.set("Authorization", `Bearer ${token}`);
    }

    const config: RequestInit = {
        ...options,
        headers,
    };

    const url = `${API_URL}${endpoint}`;
    console.log("Fetching:", url, config); // DEBUG LOG
    const response = await fetch(url, config);

    if (!response.ok) {
        // Handle 401 Unauthorized specifically if needed
        if (response.status === 401) {
            console.error("Unauthorized access - maybe token expired?");
        }
        throw new Error(`API Request failed: ${response.status} ${response.statusText}`);
    }

    return response.json();
}
