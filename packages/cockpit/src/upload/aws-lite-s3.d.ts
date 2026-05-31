// Ambient declaration for the @aws-lite/s3 plugin (DAT-386).
//
// @aws-lite/s3 ships no .d.ts (only ESM). We pass it to awsLite({ plugins })
// purely as an opaque plugin object and never call into it directly — the typed
// surface we use is S3.PutObject on the CLIENT (typed locally in s3-upload.ts).
// Declaring the module keeps the import type-checked without `any` leaking.
declare module "@aws-lite/s3" {
	const plugin: unknown;
	export default plugin;
}
