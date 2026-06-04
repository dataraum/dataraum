// Display helpers for storage URIs, e.g. `s3://bucket/uploads/<id>/<filename>`.
//
// We don't scrub the path from the underlying data — keeping the full s3:// URI
// in tool JSON is fine, it's not surprising how files are stored. But the UI
// shows the human parts: split on "/", the last segment is the filename and the
// second-to-last is the upload id; the bucket/prefix plumbing stays hidden.

function segments(uri: string): string[] {
	return uri.split("/").filter(Boolean);
}

/** The filename (last path segment); the whole string if it has no slashes. */
export function fileName(uri: string): string {
	return segments(uri).at(-1) ?? uri;
}

/** The upload id (second-to-last segment), or null if the path is too shallow. */
export function fileIdSegment(uri: string): string | null {
	const parts = segments(uri);
	return parts.length >= 2 ? (parts.at(-2) ?? null) : null;
}
