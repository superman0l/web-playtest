"use strict";

const chunkCacheName = "sts2-web-parts-v1";
const retryDelaysMs = [600, 1800];

async function waitForVisiblePage(signal) {
	if (signal.aborted) throw signal.reason;
	if (document.visibilityState === "visible") return;
	await new Promise((resolve, reject) => {
		function cleanUp() {
			document.removeEventListener("visibilitychange", onVisibilityChange);
			signal.removeEventListener("abort", onAbort);
		}
		function onVisibilityChange() {
			if (document.visibilityState !== "visible") return;
			cleanUp();
			resolve();
		}
		function onAbort() {
			cleanUp();
			reject(signal.reason);
		}
		document.addEventListener("visibilitychange", onVisibilityChange);
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

function keepScreenAwakeDuringStartup() {
	if (!navigator.wakeLock) return () => {};
	let active = true;
	let pending = false;
	let lock;
	async function requestLock() {
		if (!active || pending || (lock && !lock.released) || document.visibilityState !== "visible") return;
		pending = true;
		try {
			const requested = await navigator.wakeLock.request("screen");
			if (active && document.visibilityState === "visible") lock = requested;
			else await requested.release();
		} catch {
			// Unsupported devices and low-power policies can deny the lock.
		} finally {
			pending = false;
		}
	}
	document.addEventListener("visibilitychange", requestLock);
	document.addEventListener("pointerdown", requestLock);
	void requestLock();
	return () => {
		active = false;
		document.removeEventListener("visibilitychange", requestLock);
		document.removeEventListener("pointerdown", requestLock);
		if (lock && !lock.released) void lock.release().catch(() => {});
	};
}

async function startChunkedWebGame(engine, assets, options) {
	if (typeof DecompressionStream !== "function") {
		throw new Error("This browser does not support the game's gzip asset format.");
	}

	const originalFetch = globalThis.fetch;
	const fetchPart = originalFetch.bind(globalThis);
	const assetUrls = new Map(Object.entries(assets).map(([path, asset]) => [
		new URL(path, document.baseURI).href, asset,
	]));
	const partUrls = new Set(Object.values(assets).flatMap(asset => asset.parts.map(
		path => new URL(path, document.baseURI).href,
	)));
	const downloads = new AbortController();
	let failStartup;
	const startupFailure = new Promise((_, reject) => { failStartup = reject; });
	let chunkCache;
	const stopKeepingScreenAwake = keepScreenAwakeDuringStartup();
	try {
		chunkCache = await globalThis.caches?.open(chunkCacheName);
	} catch {
		// Browser storage is optional. Keep streaming if it is unavailable.
	}

	async function downloadCompletePart(path, url) {
		const signal = downloads.signal;
		for (let attempt = 0; ; attempt++) {
			try {
				const response = await fetchPart(url, { signal });
				if (response.status !== 200 || !response.body) {
					throw new Error(`Cannot load ${path} (HTTP ${response.status}).`);
				}
				// Feed gzip only after this part finishes so a retry cannot repeat bytes already sent to the decoder.
				return { response, data: await response.blob() };
			} catch (error) {
				const retryable = error instanceof TypeError || error.name === "NetworkError" || error.name === "AbortError";
				if (signal.aborted || !retryable) throw error;
				if (attempt === retryDelaysMs.length) {
					throw new Error(`Cannot load ${path}: ${error.message}`, { cause: error });
				}
				await waitForVisiblePage(signal);
				await new Promise(resolve => setTimeout(resolve, retryDelaysMs[attempt]));
				await waitForVisiblePage(signal);
			}
		}
	}

	function openAsset(asset) {
		let partIndex = 0;
		let reader;
		let savedPart;
		const compressed = new ReadableStream({
			async pull(controller) {
				while (true) {
					if (!reader) {
						if (partIndex === asset.parts.length) {
							controller.close();
							return;
						}
						const path = asset.parts[partIndex++];
						const url = new URL(path, document.baseURI).href;
						const cachedPart = await chunkCache?.match(url).catch(() => undefined);
						if (cachedPart?.body) {
							reader = cachedPart.body.getReader();
						} else {
							const { response, data } = await downloadCompletePart(path, url);
							if (chunkCache && response.type === "basic" && !response.redirected) {
								savedPart = chunkCache.put(url, new Response(data)).catch(() => {});
							}
							reader = data.stream().getReader();
						}
					}
					const { done, value } = await reader.read();
					if (!done) {
						controller.enqueue(value);
						return;
					}
					// Bound the cache write to one part per stream and let completed parts survive reloads.
					await savedPart;
					savedPart = undefined;
					reader.releaseLock();
					reader = undefined;
				}
			},
			cancel(reason) {
				return reader?.cancel(reason);
			},
		});
		return compressed.pipeThrough(new DecompressionStream("gzip"));
	}

	// Godot's preloader and the linked WASM loader use the original asset URLs.
	const assetFetch = async (input, init) => {
		const request = input instanceof Request ? input : undefined;
		const url = new URL(request?.url ?? input, document.baseURI).href;
		const asset = assetUrls.get(url);
		if (!asset) {
			return fetchPart(input, init);
		}
		// Errors on either stream must reach the shell even if Godot's loader stalls.
		const reader = openAsset(asset).getReader();
		const body = new ReadableStream({
			async pull(controller) {
				try {
					const { done, value } = await reader.read();
					if (done) {
						reader.releaseLock();
						controller.close();
					} else {
						controller.enqueue(value);
					}
				} catch (error) {
					failStartup(error);
					controller.error(error);
				}
			},
			cancel(reason) {
				return reader.cancel(reason);
			},
		});
		return new Response(body, {
			headers: { "Content-Type": asset.contentType, "Content-Length": String(asset.size) },
		});
	};
	globalThis.fetch = assetFetch;
	try {
		await Promise.race([engine.startGame(options), startupFailure]);
		// Once the current build has loaded, free space from earlier builds on this origin.
		void chunkCache?.keys().then(keys => Promise.all(keys
			.filter(key => !partUrls.has(key.url))
			.map(key => chunkCache.delete(key)),
		)).catch(() => {});
	} finally {
		stopKeepingScreenAwake();
		downloads.abort();
		if (globalThis.fetch === assetFetch) {
			globalThis.fetch = originalFetch;
		}
	}
}
