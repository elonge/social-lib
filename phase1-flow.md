Updated user flows
A. New user (no account)

User lands on the landing page.

Primary CTA: “Scan your bookshelf” (or equivalent).

User uploads a photo of their bookshelf.

System analyzes the image and presents a Staged List, grouped into:

High Confidence

Review Needed

Unidentified Spines

User reviews and optionally edits the staged results:

Confirms obvious matches

Fixes uncertain ones

Tags or ignores unidentified books

CTA: “Save your library” / “Create your library”

User is prompted to sign up to save or share.

After signup, the staged list is committed as their first library.

Key idea: the user experiences the value and the honesty of the system before being asked to register.

B. Existing user

User signs in and sees their Library (saved books only).

From the library page, they can:

Add books manually

Upload a new bookshelf photo

Uploading a photo triggers the same Staged List flow:

High Confidence

Review Needed

Unidentified Spines

The system automatically:

Flags duplicates against the existing library

Excludes already-owned books from the staged list (or marks them as duplicates)

User reviews the staged list and commits approved books.

The library updates with the new additions.

C. Visitor from a shared link

User opens a shared library link.

They see a read-only view of the library.

Clear CTA: “Create your own library” / “Scan your bookshelf”

Clicking the CTA starts the new user upload → staged list flow.

Signup is required only when saving.

Subtle but important UX implications (intentional)

Recognition is framed as probabilistic, not binary
This builds trust and reduces disappointment.

Users feel in control
They are approving additions, not fixing mistakes.

The system never “fails silently”
Unidentified spines are acknowledged explicitly.

The Staged List becomes a first-class concept
It’s the same flow for:

New users

Existing users

Incremental uploads

This consistency will pay off later as features expand.