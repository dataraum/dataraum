// Content digest for staged uploads — dedup / "already uploaded" detection.
//
// An upload is content-addressed by this digest: identical bytes (in the same
// workspace) hash to the same value, so the upload route lands them at the same
// `uploads/<digest>/` key and skips the re-PUT instead of accumulating a fresh
// `uploads/<uuid>/` copy each time (S3 was a sink-hole before — files vanished
// with the chat conversation and re-uploads duplicated silently).
//
// Two-tier for speed on large files: a file under 8 MB is hashed whole; a larger
// one is hashed as a Merkle-style roll-up — SHA-1 over each 8 MB slice, then a
// final SHA-1 over the concatenated slice digests. Either way the workspace id
// (+ byte length) is mixed in as a salt, so the same bytes digest differently
// per workspace (workspace-scoped dedup). SHA-1 here is a FAST content
// fingerprint, not a security primitive.

const MAX_SLICE_BYTES = 8e6; // 8 MB — whole-file threshold and slice size
const SHA1_BYTES = 20; // SHA-1 digest length in bytes (160 bits)

/** SHA-1 of `value` as raw bytes. */
async function quickHash(value: BufferSource): Promise<Uint8Array> {
	const hashBuffer = await crypto.subtle.digest("SHA-1", value);
	return new Uint8Array(hashBuffer);
}

/** SHA-1 of `value` as a lowercase hex string. */
async function quickHashToString(value: BufferSource): Promise<string> {
	const bytes = await quickHash(value);
	return Array.from(bytes)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

/**
 * Workspace-scoped content digest of `bytes`. Deterministic: the same bytes +
 * the same `salt` always produce the same hex string (→ dedup); different bytes,
 * a different length, or a different workspace (salt) digest differently.
 */
export async function digestBytes(
	bytes: Uint8Array,
	salt: string,
): Promise<string> {
	const salted = new TextEncoder().encode(salt + bytes.byteLength);

	if (bytes.byteLength < MAX_SLICE_BYTES) {
		// Small file: hash the bytes followed by the salt in one pass.
		const buf = new Uint8Array(bytes.byteLength + salted.length);
		buf.set(bytes, 0);
		buf.set(salted, bytes.byteLength);
		return quickHashToString(buf);
	}

	// Large file: SHA-1 each slice (awaited — the roll-up MUST see every slice
	// digest before it hashes them, or the result is non-deterministic), then
	// hash the concatenated slice digests followed by the salt.
	const sliceCount = Math.ceil(bytes.byteLength / MAX_SLICE_BYTES);
	const sliceDigests = await Promise.all(
		Array.from({ length: sliceCount }, (_, i) =>
			// Copy the slice into a fresh ArrayBuffer-backed view: a `Buffer`-backed
			// subarray is `Uint8Array<ArrayBufferLike>`, which TS 5.7+ rejects as a
			// WebCrypto `BufferSource` (it could be SharedArrayBuffer-backed).
			quickHash(
				new Uint8Array(
					bytes.subarray(i * MAX_SLICE_BYTES, (i + 1) * MAX_SLICE_BYTES),
				),
			),
		),
	);
	const rolled = new Uint8Array(sliceCount * SHA1_BYTES + salted.length);
	for (let i = 0; i < sliceDigests.length; i++) {
		rolled.set(sliceDigests[i], i * SHA1_BYTES);
	}
	rolled.set(salted, sliceCount * SHA1_BYTES);
	return quickHashToString(rolled);
}

/** Workspace-scoped content digest of an uploaded `File`. */
export async function digestFile(file: File, salt: string): Promise<string> {
	return digestBytes(new Uint8Array(await file.arrayBuffer()), salt);
}
