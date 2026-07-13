/**
 * Resolve after `ms` milliseconds — the single shared delay helper (AGENTS §16.1),
 * for letting a real short timer (a {@link createTimeout} expiry) elapse instead of
 * inlining a `setTimeout` promise per test.
 *
 * @param ms - Milliseconds to wait; defaults to `0` (a macrotask turn)
 * @returns A promise that resolves once the delay elapses
 */
export function waitForDelay(ms = 0): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}
