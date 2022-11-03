import type {Context, Next} from 'koa';
import type {RateLimiterRes} from 'rate-limiter-flexible';

import rateLimiter from '../../ratelimiter.js';
import getClientIp from '../../get-client-ip.js';

const setResponseHeaders = (ctx: Context, response: RateLimiterRes) => {
	ctx.set('X-RateLimit-Reset', `${Math.round(response.msBeforeNext / 1000)}`);
	ctx.set('X-RateLimit-Limit', `${rateLimiter.points}`);
	ctx.set('X-RateLimit-Remaining', `${response.remainingPoints}`);
};

// eslint-disable-next-line unicorn/prevent-abbreviations
const methodsWhitelist = new Set(['GET', 'HEAD', 'OPTIONS']);

export const rateLimitHandler = () => async (ctx: Context, next: Next) => {
	if (methodsWhitelist.has(ctx.method)) {
		return next();
	}

	try {
		const response = await rateLimiter.consume(getClientIp(ctx.req) ?? '');
		setResponseHeaders(ctx, response);
	} catch (error: unknown) { // Ts requires 'unknown' for errors
		setResponseHeaders(ctx, error as RateLimiterRes);
		ctx.status = 429;
		ctx.body = 'Too Many Requests';
		return;
	}

	await next();
};
