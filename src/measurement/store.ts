import config from 'config';
import cryptoRandomString from 'crypto-random-string';
import type {Probe} from '../probe/types.js';
import type {RedisClient} from '../lib/redis/client.js';
import {getRedisClient} from '../lib/redis/client.js';
import type {MeasurementRecord, MeasurementResultMessage, NetworkTest} from './types.js';

export const getMeasurementKey = (id: string, suffix: 'probes_awaiting' | undefined = undefined): string => {
	let key = `gp:measurement:${id}`;

	if (suffix) {
		key += `:${suffix}`;
	}

	return key;
};

export class MeasurementStore {
	constructor(private readonly redis: RedisClient) {}

	async getMeasurementResults(id: string): Promise<MeasurementRecord> {
		return await this.redis.json.get(getMeasurementKey(id)) as never;
	}

	async createMeasurement(test: NetworkTest, probesCount: number): Promise<string> {
		const id = cryptoRandomString({length: 16, type: 'alphanumeric'});
		const key = getMeasurementKey(id);

		const probesAwaitingTtl = config.get<number>('measurement.timeout') + 5;
		const multi = this.redis.multi();
		// eslint-disable-next-line @typescript-eslint/naming-convention
		multi.set(getMeasurementKey(id, 'probes_awaiting'), probesCount, {EX: probesAwaitingTtl});
		multi.json.set(key, '$', {
			id,
			type: test.type,
			status: 'in-progress',
			createdAt: Date.now(),
			updatedAt: Date.now(),
			probesCount,
			results: {},
		});
		multi.expire(key, config.get<number>('measurement.resultTTL'));
		await multi.exec();

		return id;
	}

	async storeMeasurementProbe(measurementId: string, probeId: string, probe: Probe): Promise<void> {
		const key = getMeasurementKey(measurementId);
		const multi = this.redis.multi();
		multi.json.set(key, `$.results.${probeId}`, {
			probe: {
				continent: probe.location.continent,
				region: probe.location.region,
				country: probe.location.country,
				state: probe.location.state ?? null,
				city: probe.location.city,
				asn: probe.location.asn,
				longitude: probe.location.longitude,
				latitude: probe.location.latitude,
				network: probe.location.network,
				resolvers: probe.resolvers,
			},
			result: {rawOutput: ''},
		});
		multi.json.set(key, '$.updatedAt', Date.now());
		await multi.exec();
	}

	async storeMeasurementProgress(data: MeasurementResultMessage): Promise<void> {
		const key = getMeasurementKey(data.measurementId);
		const multi = this.redis.multi();

		data.overwrite
			? multi.json.set(key, `$.results.${data.testId}.result.rawOutput`, data.result.rawOutput)
			: multi.json.strAppend(key, `$.results.${data.testId}.result.rawOutput`, data.result.rawOutput);
		multi.json.set(key, '$.updatedAt', Date.now());
		await multi.exec();
	}

	async storeMeasurementResult(data: MeasurementResultMessage): Promise<void> {
		const key = getMeasurementKey(data.measurementId);
		const multi = this.redis.multi();
		multi.json.set(key, `$.results.${data.testId}.result`, data.result);
		multi.json.set(key, '$.updatedAt', Date.now());
		multi.decr(`${key}:probes_awaiting`);
		await multi.exec();
	}

	async markFinished(id: string): Promise<void> {
		const key = getMeasurementKey(id);
		const multi = this.redis.multi();
		multi.json.set(key, '$.status', 'finished');
		multi.json.set(key, '$.updatedAt', Date.now());
		await multi.exec();
	}
}

let store: MeasurementStore;

export const getMeasurementStore = () => {
	if (!store) {
		store = new MeasurementStore(getRedisClient());
	}

	return store;
};
