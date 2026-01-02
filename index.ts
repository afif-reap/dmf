import { createWriteStream, mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { faker } from "@faker-js/faker";

const DEFAULT_FILES = {
	card: "from-db/card.csv",
	business: "from-db/business.csv",
	budget: "from-db/budget.csv",
	enums: "from-db/enums.json",
};

const DEFAULT_ROWS = {
	card: 1000,
	business: 200,
	budget: 400,
};

const DEFAULT_MAX_CARDS_PER_BUSINESS = 1000;
const DEFAULT_BUDGETS_PER_BUSINESS = Math.max(
	1,
	Math.round(DEFAULT_ROWS.budget / DEFAULT_ROWS.business),
);

const DEFAULT_SAMPLE_ROWS = 200;

type TableName = "business" | "budget" | "card";

type ColumnType =
	| "uuid"
	| "boolean"
	| "number"
	| "timestamp"
	| "date"
	| "exp_date"
	| "json"
	| "string";

type StringPattern =
	| { kind: "numeric"; length: number }
	| { kind: "prefix_digits"; prefix: string; digits: number }
	| {
			kind: "prefix_separator";
			prefix: string;
			separator: string;
			suffixLength: number;
	  }
	| { kind: "hex"; length: number }
	| { kind: "masked_pan" }
	| { kind: "alphanumeric"; minLength: number; maxLength: number };

type ColumnProfile = {
	name: string;
	type: ColumnType;
	nullRate: number;
	minLength: number;
	maxLength: number;
	enumValues?: string[];
	jsonSample?: unknown;
	numberScale?: number;
	numberMin?: number;
	numberMax?: number;
	dateMin?: Date;
	dateMax?: Date;
	stringPattern?: StringPattern;
};

type GenerationContext = {
	businessIds: string[];
	budgetIds: string[];
	applicationIds: string[];
	budgetsByBusiness: Map<string, string[]>;
};

type BusinessPlan = {
	id: string;
	budgetIds: string[];
	cardCount: number;
};

type RowSeed = (index: number) => Partial<Record<string, string | null>>;

type Args = {
	outDir: string;
	sampleRows: number;
	seed?: number;
	copyMode: "copy" | "psql";
	maxCardsPerBusiness: number;
	progressEvery: number;
	enumFile?: string;
	explicitRows: {
		card: boolean;
		business: boolean;
		budget: boolean;
	};
	rows: {
		card: number;
		business: number;
		budget: number;
	};
	inputFiles: {
		card: string;
		business: string;
		budget: string;
	};
};

const ENUM_HINTS = [
	"status",
	"type",
	"plan",
	"industry",
	"currency",
	"country",
	"client_type",
];

const CURRENCY_CODES = ["344", "702", "764", "840", "978"];
const COUNTRY_CODES = ["HK", "SG", "US", "AX", "AU", "FR", "GB"];

function printUsage(): void {
	const usage = `Usage: bun run index.ts [options]

Options:
  --rows <n>                 Rows for all tables (default 1000/200/400)
  --cards <n>                Rows for card table (alias for --card-rows)
  --card-rows <n>            Rows for card table
  --business-rows <n>        Rows for business table
  --budget-rows <n>          Rows for budget table
  --max-cards-per-business <n> Max cards per business (default 1000)
  --sample-rows <n>          Sample rows for inference (default 200)
  --progress-every <n>       Log progress every N rows (default 100000)
  --enum-file <path>         JSON map of enum values to use (default from-db/enums.json)
  --seed <n>                 Faker seed for repeatable data
  --out-dir <path>           Output directory (default ./out)
  --copy-mode <copy|psql>    Emit COPY or \copy statements (default psql)
  --card-file <path>         Input card CSV (default from-db/card_202601021115.csv)
  --business-file <path>     Input business CSV (default from-db/business_202601021115.csv)
  --budget-file <path>       Input budget CSV (default from-db/budget_202601021115.csv)
  --help                     Show usage


Example:
  bun run index.ts --rows 5000 --out-dir ./out
  bun run index.ts --cards 200000 --max-cards-per-business 1000
  bun run index.ts --card-rows 20000 --budget-rows 5000 --business-rows 1000

`;
	console.log(usage);
}

function parseArgs(argv: string[]): Args | null {
	const args: Args = {
		outDir: "out",
		sampleRows: DEFAULT_SAMPLE_ROWS,
		copyMode: "psql",
		maxCardsPerBusiness: DEFAULT_MAX_CARDS_PER_BUSINESS,
		progressEvery: 100000,
		enumFile: DEFAULT_FILES.enums,
		explicitRows: {
			card: false,
			business: false,
			budget: false,
		},
		rows: { ...DEFAULT_ROWS },
		inputFiles: { ...DEFAULT_FILES },
	};

	const getValue = (index: number): string | undefined => {
		if (index + 1 >= argv.length) return undefined;
		return argv[index + 1];
	};

	for (let i = 0; i < argv.length; i += 1) {
		const arg = argv[i];
		if (arg === "--help" || arg === "-h") {
			return null;
		}
		if (arg === "--rows") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count)) {
					args.rows = { card: count, business: count, budget: count };
					args.explicitRows = { card: true, business: true, budget: true };
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--card-rows" || arg === "--cards") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count)) {
					args.rows.card = count;
					args.explicitRows.card = true;
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--business-rows") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count)) {
					args.rows.business = count;
					args.explicitRows.business = true;
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--budget-rows") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count)) {
					args.rows.budget = count;
					args.explicitRows.budget = true;
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--sample-rows") {
			const value = getValue(i);
			if (value) args.sampleRows = Number.parseInt(value, 10);
			i += 1;
			continue;
		}
		if (arg === "--progress-every") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count) && count > 0) {
					args.progressEvery = count;
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--enum-file") {
			const value = getValue(i);
			if (value) args.enumFile = value;
			i += 1;
			continue;
		}
		if (arg === "--seed") {
			const value = getValue(i);
			if (value) args.seed = Number.parseInt(value, 10);
			i += 1;
			continue;
		}
		if (arg === "--out-dir") {
			const value = getValue(i);
			if (value) args.outDir = value;
			i += 1;
			continue;
		}
		if (arg === "--copy-mode") {
			const value = getValue(i);
			if (value === "copy" || value === "psql") {
				args.copyMode = value;
			}
			i += 1;
			continue;
		}
		if (arg === "--max-cards-per-business") {
			const value = getValue(i);
			if (value) {
				const count = Number.parseInt(value, 10);
				if (Number.isFinite(count) && count > 0) {
					args.maxCardsPerBusiness = count;
				}
			}
			i += 1;
			continue;
		}
		if (arg === "--card-file") {
			const value = getValue(i);
			if (value) args.inputFiles.card = value;
			i += 1;
			continue;
		}
		if (arg === "--business-file") {
			const value = getValue(i);
			if (value) args.inputFiles.business = value;
			i += 1;
			continue;
		}
		if (arg === "--budget-file") {
			const value = getValue(i);
			if (value) args.inputFiles.budget = value;
			i += 1;
			continue;
		}
	}

	return args;
}

async function readLines(
	filePath: string,
	maxLines: number,
): Promise<string[]> {
	const file = Bun.file(filePath);
	const reader = file.stream().getReader();
	const decoder = new TextDecoder();
	const lines: string[] = [];
	let buffer = "";

	while (lines.length < maxLines) {
		const { value, done } = await reader.read();
		if (done) break;
		buffer += decoder.decode(value, { stream: true });
		const parts = buffer.split(/\r?\n/);
		buffer = parts.pop() ?? "";
		for (const line of parts) {
			if (line.length === 0) continue;
			lines.push(line);
			if (lines.length >= maxLines) break;
		}
	}

	if (lines.length < maxLines && buffer.length > 0) {
		lines.push(buffer);
	}

	return lines;
}

function parseCsvLine(line: string): string[] {
	const values: string[] = [];
	let current = "";
	let inQuotes = false;

	for (let i = 0; i < line.length; i += 1) {
		const char = line[i];
		if (char === '"') {
			const next = line[i + 1];
			if (inQuotes && next === '"') {
				current += '"';
				i += 1;
			} else {
				inQuotes = !inQuotes;
			}
			continue;
		}
		if (char === "," && !inQuotes) {
			values.push(current);
			current = "";
			continue;
		}
		current += char;
	}
	values.push(current);
	return values;
}

function csvEscape(value: string | null): string {
	if (value === null) return "";
	const escaped = value.replace(/"/g, '""');
	return `"${escaped}"`;
}

function sqlEscape(value: string): string {
	return value.replace(/'/g, "''");
}

function buildCopyStatement(
	tableName: TableName,
	csvPath: string,
	mode: "copy" | "psql",
): string {
	const escapedPath = sqlEscape(csvPath);
	if (mode === "psql") {
		return `\\copy ${tableName} FROM '${escapedPath}' WITH (FORMAT csv, HEADER true);`;
	}
	return `COPY ${tableName} FROM '${escapedPath}' WITH (FORMAT csv, HEADER true);`;
}

function isUuid(value: string): boolean {
	return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
		value,
	);
}

function isBoolean(value: string): boolean {
	return value === "true" || value === "false";
}

function isNumber(value: string): boolean {
	return /^-?\d+(\.\d+)?$/.test(value);
}

function isTimestamp(value: string): boolean {
	return /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/.test(value);
}

function isDate(value: string): boolean {
	return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function isExpDate(value: string): boolean {
	return /^\d{2}\/\d{2}$/.test(value);
}

function isJsonLike(value: string): boolean {
	const trimmed = value.trim();
	if (!trimmed) return false;
	if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) return false;
	try {
		JSON.parse(trimmed);
		return true;
	} catch {
		return false;
	}
}

function parseJson(value: string): unknown | null {
	try {
		return JSON.parse(value);
	} catch {
		return null;
	}
}

function inferStringPattern(values: string[]): StringPattern | undefined {
	if (values.length === 0) return undefined;

	if (values.every((value) => /^\d+$/.test(value))) {
		const length = Math.round(
			values.reduce((sum, v) => sum + v.length, 0) / values.length,
		);
		return { kind: "numeric", length };
	}

	if (values.every((value) => /^\d{6}\*{6}\d{4}$/.test(value))) {
		return { kind: "masked_pan" };
	}

	const prefixDigits = values.map((value) => value.match(/^([A-Za-z]+)(\d+)$/));
	if (prefixDigits.every((match) => match && match[1])) {
		const prefix = prefixDigits[0]?.[1] ?? "";
		if (prefix && prefixDigits.every((match) => match?.[1] === prefix)) {
			const digits = Math.round(
				prefixDigits.reduce(
					(sum, match) => sum + (match?.[2]?.length ?? 0),
					0,
				) / prefixDigits.length,
			);
			return { kind: "prefix_digits", prefix, digits };
		}
	}

	const prefixSeparator = values.map((value) =>
		value.match(/^([A-Za-z]+)([_-])([A-Za-z0-9]+)$/),
	);
	if (prefixSeparator.every((match) => match && match[1])) {
		const prefix = prefixSeparator[0]?.[1] ?? "";
		const separator = prefixSeparator[0]?.[2] ?? "_";
		if (
			prefix &&
			prefixSeparator.every((match) => match?.[1] === prefix) &&
			prefixSeparator.every((match) => match?.[2] === separator)
		) {
			const suffixLength = Math.round(
				prefixSeparator.reduce(
					(sum, match) => sum + (match?.[3]?.length ?? 0),
					0,
				) / prefixSeparator.length,
			);
			return { kind: "prefix_separator", prefix, separator, suffixLength };
		}
	}

	if (values.every((value) => /^[0-9a-f]{32}$/i.test(value))) {
		return { kind: "hex", length: 32 };
	}

	if (values.every((value) => /^[A-Za-z0-9]+$/.test(value))) {
		const lengths = values.map((value) => value.length);
		const minLength = Math.min(...lengths);
		const maxLength = Math.max(...lengths);
		return { kind: "alphanumeric", minLength, maxLength };
	}

	return undefined;
}

function shouldUseEnum(name: string): boolean {
	const lowered = name.toLowerCase();
	if (lowered.includes("name")) return false;
	if (lowered.endsWith("_id") || lowered.endsWith("_uuid")) return false;
	if (lowered.includes("token") || lowered.includes("pass")) return false;
	return ENUM_HINTS.some((hint) => lowered.includes(hint));
}

function inferColumnProfile(name: string, values: string[]): ColumnProfile {
	const nonEmpty = values.filter((value) => value !== "");
	const nullRate =
		values.length === 0
			? 0.1
			: (values.length - nonEmpty.length) / values.length;
	const samples = nonEmpty.slice(0, 50);
	const minLength = samples.reduce(
		(min, value) => Math.min(min, value.length),
		Number.POSITIVE_INFINITY,
	);
	const maxLength = samples.reduce(
		(max, value) => Math.max(max, value.length),
		0,
	);
	const enumValues = shouldUseEnum(name)
		? Array.from(new Set(samples)).filter(
				(value) => value.length > 0 && value.length < 64,
			)
		: undefined;
	const boundedEnumValues =
		enumValues && enumValues.length > 0 && enumValues.length <= 20
			? enumValues
			: undefined;

	if (samples.length > 0 && samples.every(isBoolean)) {
		return {
			name,
			type: "boolean",
			nullRate,
			minLength,
			maxLength,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isUuid)) {
		return {
			name,
			type: "uuid",
			nullRate,
			minLength,
			maxLength,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isTimestamp)) {
		const dates = samples.map(
			(value) => new Date(value.replace(" ", "T") + "Z"),
		);
		const dateMin = new Date(Math.min(...dates.map((date) => date.getTime())));
		const dateMax = new Date(Math.max(...dates.map((date) => date.getTime())));
		return {
			name,
			type: "timestamp",
			nullRate,
			minLength,
			maxLength,
			dateMin,
			dateMax,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isDate)) {
		const dates = samples.map((value) => new Date(value + "T00:00:00Z"));
		const dateMin = new Date(Math.min(...dates.map((date) => date.getTime())));
		const dateMax = new Date(Math.max(...dates.map((date) => date.getTime())));
		return {
			name,
			type: "date",
			nullRate,
			minLength,
			maxLength,
			dateMin,
			dateMax,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isExpDate)) {
		return {
			name,
			type: "exp_date",
			nullRate,
			minLength,
			maxLength,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isNumber)) {
		const nums = samples.map((value) => Number(value));
		const numberMin = Math.min(...nums);
		const numberMax = Math.max(...nums);
		const scales = samples
			.map((value) => value.split(".")[1]?.length ?? 0)
			.filter((scale) => scale > 0);
		const numberScale = scales.length > 0 ? Math.max(...scales) : 0;
		return {
			name,
			type: "number",
			nullRate,
			minLength,
			maxLength,
			numberScale,
			numberMin,
			numberMax,
			enumValues: boundedEnumValues,
		};
	}

	if (samples.length > 0 && samples.every(isJsonLike)) {
		const jsonSample = parseJson(samples[0] ?? "{}") ?? {};
		return {
			name,
			type: "json",
			nullRate,
			minLength,
			maxLength,
			jsonSample,
			enumValues: boundedEnumValues,
		};
	}

	const stringPattern = inferStringPattern(samples);

	return {
		name,
		type: "string",
		nullRate,
		minLength: Number.isFinite(minLength) ? minLength : 0,
		maxLength,
		enumValues: boundedEnumValues,
		stringPattern,
	};
}

function inferProfiles(header: string[], rows: string[][]): ColumnProfile[] {
	const columns = header.map((name) => ({ name, values: [] as string[] }));
	for (const row of rows) {
		if (row.length !== header.length) continue;
		for (let i = 0; i < row.length; i += 1) {
			columns[i]?.values.push(row[i] ?? "");
		}
	}
	return columns.map((column) =>
		inferColumnProfile(column.name, column.values),
	);
}

type EnumOverrides = Record<string, string[]>;

async function loadEnumOverrides(filePath?: string): Promise<EnumOverrides> {
	if (!filePath) return {};
	const resolved = resolve(filePath);
	const file = Bun.file(resolved);
	if (!(await file.exists())) {
		throw new Error(`Enum file not found: ${resolved}`);
	}
	const raw = await file.text();
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch (error) {
		throw new Error(`Enum file is not valid JSON: ${resolved}`);
	}
	if (!parsed || typeof parsed !== "object") {
		throw new Error(`Enum file must be a JSON object: ${resolved}`);
	}
	const overrides: EnumOverrides = {};
	for (const [key, value] of Object.entries(parsed)) {
		if (Array.isArray(value)) {
			overrides[key] = value.map((item) => String(item));
		}
	}
	return overrides;
}

function applyEnumOverrides(
	profiles: ColumnProfile[],
	tableName: TableName,
	enumOverrides: EnumOverrides,
): ColumnProfile[] {
	if (!enumOverrides || Object.keys(enumOverrides).length === 0) {
		return profiles;
	}
	return profiles.map((profile) => {
		const tableKey = `${tableName}.${profile.name}`;
		const columnKey = profile.name;
		const override = enumOverrides[tableKey] ?? enumOverrides[columnKey];
		if (!override || override.length === 0) return profile;
		return { ...profile, enumValues: override };
	});
}

function formatDate(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, "0");
	const day = String(date.getUTCDate()).padStart(2, "0");
	return `${year}-${month}-${day}`;
}

function formatTimestamp(date: Date): string {
	const year = date.getUTCFullYear();
	const month = String(date.getUTCMonth() + 1).padStart(2, "0");
	const day = String(date.getUTCDate()).padStart(2, "0");
	const hours = String(date.getUTCHours()).padStart(2, "0");
	const minutes = String(date.getUTCMinutes()).padStart(2, "0");
	const seconds = String(date.getUTCSeconds()).padStart(2, "0");
	const ms = String(date.getUTCMilliseconds()).padStart(3, "0");
	return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${ms}`;
}

function randomDateBetween(min: Date, max: Date): Date {
	const minTime = min.getTime();
	const maxTime = max.getTime();
	const time = faker.number.int({ min: minTime, max: maxTime });
	return new Date(time);
}

function randomNumericString(length: number): string {
	return faker.string.numeric({ length });
}

function randomHex(length: number): string {
	const chars = "0123456789abcdef";
	let out = "";
	for (let i = 0; i < length; i += 1) {
		out += chars.charAt(faker.number.int({ min: 0, max: chars.length - 1 }));
	}
	return out;
}

function maskPan(): string {
	const digits = faker.string.numeric({ length: 16 });
	return `${digits.slice(0, 6)}******${digits.slice(-4)}`;
}

function fakeJsonValue(value: unknown, keyName = ""): unknown {
	if (value === null) return null;
	if (Array.isArray(value)) {
		if (value.length === 0) return [];
		const sample = value[0];
		const length = faker.number.int({ min: 0, max: 2 });
		return Array.from({ length }, () => fakeJsonValue(sample, keyName));
	}
	if (typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>);
		const output: Record<string, unknown> = {};
		for (const [key, inner] of entries) {
			output[key] = fakeJsonValue(inner, key);
		}
		return output;
	}
	if (typeof value === "boolean") {
		return faker.datatype.boolean();
	}
	if (typeof value === "number") {
		return faker.number.float({ min: 0, max: 1000, fractionDigits: 2 });
	}
	if (typeof value !== "string") return value;

	const lowered = keyName.toLowerCase();
	if (lowered.includes("email")) return faker.internet.email().toLowerCase();
	if (lowered.includes("phone")) return faker.phone.number();
	if (lowered.includes("country"))
		return faker.helpers.arrayElement(COUNTRY_CODES);
	if (lowered.includes("postal")) return faker.location.zipCode("#####");
	if (lowered.includes("city")) return faker.location.city();
	if (lowered.includes("line")) return faker.location.streetAddress();
	if (lowered.includes("first")) return faker.person.firstName();
	if (lowered.includes("last")) return faker.person.lastName();
	if (lowered.includes("dob"))
		return formatDate(
			faker.date.birthdate({ mode: "year", min: 1970, max: 2002 }),
		);
	if (lowered.includes("id")) return faker.string.alphanumeric({ length: 12 });

	if (value.length === 0) return "";
	if (value.length <= 12)
		return faker.string.alphanumeric({ length: value.length });
	return faker.string.alphanumeric({ length: Math.min(value.length, 24) });
}

function fakeJson(sample: unknown): string {
	const value = fakeJsonValue(sample);
	return JSON.stringify(value);
}

function clampString(value: string, maxLength: number): string {
	if (maxLength > 0 && value.length > maxLength) {
		return value.slice(0, maxLength);
	}
	return value;
}

function generateStringValue(
	profile: ColumnProfile,
	columnName: string,
): string {
	if (profile.enumValues && profile.enumValues.length > 0) {
		return faker.helpers.arrayElement(profile.enumValues);
	}

	const finalize = (value: string): string =>
		clampString(value, profile.maxLength);

	const lowered = columnName.toLowerCase();
	if (lowered.includes("name")) {
		if (
			lowered.includes("company") ||
			lowered.includes("business") ||
			lowered.includes("trade")
		) {
			return finalize(faker.company.name());
		}
		return finalize(faker.person.fullName());
	}
	if (lowered.includes("title")) return finalize(faker.commerce.productName());
	if (lowered.includes("email"))
		return finalize(faker.internet.email().toLowerCase());
	if (lowered.includes("phone")) return finalize(faker.phone.number());
	if (lowered.includes("country"))
		return finalize(faker.helpers.arrayElement(COUNTRY_CODES));
	if (lowered.includes("currency"))
		return finalize(faker.helpers.arrayElement(CURRENCY_CODES));
	if (lowered.includes("reason") || lowered.includes("message"))
		return finalize(faker.lorem.sentence());
	if (lowered.includes("address"))
		return finalize(faker.location.streetAddress());

	if (profile.stringPattern) {
		switch (profile.stringPattern.kind) {
			case "numeric":
				return finalize(randomNumericString(profile.stringPattern.length));
			case "prefix_digits":
				return finalize(
					`${profile.stringPattern.prefix}${randomNumericString(profile.stringPattern.digits)}`,
				);
			case "prefix_separator":
				return finalize(
					`${profile.stringPattern.prefix}${profile.stringPattern.separator}${faker.string.alphanumeric({ length: profile.stringPattern.suffixLength })}`,
				);
			case "hex":
				return finalize(randomHex(profile.stringPattern.length));
			case "masked_pan":
				return finalize(maskPan());
			case "alphanumeric": {
				const length = faker.number.int({
					min: profile.stringPattern.minLength,
					max: profile.stringPattern.maxLength,
				});
				return finalize(faker.string.alphanumeric({ length }));
			}
		}
	}

	const length = Math.max(6, Math.min(32, profile.maxLength || 12));
	return finalize(faker.string.alphanumeric({ length }));
}

function generateValue(
	profile: ColumnProfile,
	tableName: TableName,
	row: Record<string, string | null | undefined>,
	context: GenerationContext,
): string | null {
	const lowered = profile.name.toLowerCase();

	if (profile.name === "id") {
		return faker.string.uuid();
	}

	if (tableName === "budget" && profile.name === "business_uuid") {
		return context.businessIds.length > 0
			? faker.helpers.arrayElement(context.businessIds)
			: faker.string.uuid();
	}

	if (tableName === "card" && profile.name === "budget_id") {
		return context.budgetIds.length > 0
			? faker.helpers.arrayElement(context.budgetIds)
			: faker.string.uuid();
	}

	if (tableName === "card" && profile.name === "application_id") {
		return context.applicationIds.length > 0
			? faker.helpers.arrayElement(context.applicationIds)
			: faker.string.uuid();
	}

	if (tableName === "budget" && profile.name === "root_budget_id") {
		return row.id ?? faker.string.uuid();
	}

	if (tableName === "budget" && profile.name === "path") {
		const root = row.root_budget_id ?? row.id ?? faker.string.uuid();
		const current = row.id ?? faker.string.uuid();
		return `${root.replace(/-/g, "_")}.${current.replace(/-/g, "_")}`;
	}

	if (tableName === "card" && profile.name === "masked_pan") {
		return maskPan();
	}

	if (tableName === "card" && profile.name === "exp_date") {
		const month = faker.number.int({ min: 1, max: 12 });
		const year = faker.number.int({ min: 24, max: 30 });
		return `${String(month).padStart(2, "0")}/${String(year).padStart(2, "0")}`;
	}

	if (profile.name === "updated_at" && row.created_at) {
		const created = new Date(row.created_at.replace(" ", "T") + "Z");
		const updated = randomDateBetween(created, new Date());
		return formatTimestamp(updated);
	}

	const shouldBeNull =
		faker.number.float({ min: 0, max: 1 }) < profile.nullRate;
	if (shouldBeNull) return null;

	if (profile.enumValues && profile.enumValues.length > 0) {
		if (profile.type !== "string") {
			return faker.helpers.arrayElement(profile.enumValues);
		}
	}

	switch (profile.type) {
		case "uuid":
			return faker.string.uuid();
		case "boolean":
			return faker.datatype.boolean() ? "true" : "false";
		case "number": {
			const min = profile.numberMin ?? 0;
			const max = profile.numberMax ?? 1000;
			const value = faker.number.float({
				min,
				max,
				fractionDigits: profile.numberScale ?? 2,
			});
			return value.toFixed(profile.numberScale ?? 0);
		}
		case "timestamp": {
			const min = profile.dateMin ?? faker.date.past({ years: 3 });
			const max = profile.dateMax ?? new Date();
			return formatTimestamp(randomDateBetween(min, max));
		}
		case "date": {
			const min = profile.dateMin ?? faker.date.past({ years: 3 });
			const max = profile.dateMax ?? new Date();
			return formatDate(randomDateBetween(min, max));
		}
		case "exp_date": {
			const month = faker.number.int({ min: 1, max: 12 });
			const year = faker.number.int({ min: 24, max: 30 });
			return `${String(month).padStart(2, "0")}/${String(year).padStart(2, "0")}`;
		}
		case "json":
			return fakeJson(profile.jsonSample ?? {});
		case "string":
		default:
			return generateStringValue(profile, lowered);
	}
}

async function generateTable(
	tableName: TableName,
	profiles: ColumnProfile[],
	rowCount: number,
	outDir: string,
	context: GenerationContext,
	options?: { rowSeed?: RowSeed; progressEvery?: number },
): Promise<string> {
	mkdirSync(outDir, { recursive: true });
	const outFile = join(outDir, `${tableName}.csv`);
	const stream = createWriteStream(outFile, { encoding: "utf8" });

	stream.write(
		profiles.map((profile) => csvEscape(profile.name)).join(",") + "\n",
	);

	const progressEvery = options?.progressEvery ?? 0;
	if (rowCount > 0) {
		console.log(`Generating ${tableName} (${rowCount} rows)...`);
	}

	for (let i = 0; i < rowCount; i += 1) {
		const seededRow = options?.rowSeed ? options.rowSeed(i) : {};
		const row: Record<string, string | null | undefined> = {
			...seededRow,
		};
		const values: string[] = [];

		for (const profile of profiles) {
			const hasPreset = Object.prototype.hasOwnProperty.call(row, profile.name);
			const value = hasPreset
				? row[profile.name]
				: generateValue(profile, tableName, row, context);
			row[profile.name] = value ?? null;
			values.push(csvEscape(value ?? null));
		}

		if (tableName === "business" && row.id) {
			context.businessIds.push(row.id);
		}
		if (tableName === "budget" && row.id) {
			context.budgetIds.push(row.id);
			const businessId = row.business_uuid;
			if (businessId) {
				const existing = context.budgetsByBusiness.get(businessId) ?? [];
				existing.push(row.id);
				context.budgetsByBusiness.set(businessId, existing);
			}
		}
		if (tableName === "business" && row.business_owner_application_id) {
			context.applicationIds.push(row.business_owner_application_id);
		}

		const line = values.join(",") + "\n";
		if (!stream.write(line)) {
			await new Promise<void>((resolve) => {
				stream.once("drain", resolve);
			});
		}

		if (progressEvery > 0 && (i + 1) % progressEvery === 0) {
			console.log(`[${tableName}] ${i + 1}/${rowCount} rows generated`);
		}
	}

	if (progressEvery > 0 && rowCount > 0) {
		console.log(`[${tableName}] ${rowCount}/${rowCount} rows generated`);
	}

	await new Promise<void>((resolve, reject) => {
		stream.on("error", reject);
		stream.end(() => resolve());
	});

	return outFile;
}

async function loadProfiles(
	filePath: string,
	sampleRows: number,
): Promise<ColumnProfile[]> {
	const lines = await readLines(filePath, sampleRows + 1);
	if (lines.length === 0) {
		throw new Error(`No data found in ${filePath}`);
	}
	const header = parseCsvLine(lines[0] ?? "");
	const rows = lines.slice(1).map((line) => parseCsvLine(line));
	return inferProfiles(header, rows);
}

function allocateBudgetCounts(
	businessCount: number,
	budgetCount: number,
): number[] {
	if (businessCount <= 0) return [];
	const counts = new Array(businessCount).fill(1);
	let remaining = budgetCount - businessCount;
	let index = 0;
	while (remaining > 0) {
		counts[index % businessCount] += 1;
		remaining -= 1;
		index += 1;
	}
	return counts;
}

function allocateCardCounts(
	businessCount: number,
	cardCount: number,
	maxCardsPerBusiness: number,
): number[] {
	if (businessCount <= 0) return [];
	const counts = new Array(businessCount).fill(0);
	let remaining = cardCount;
	if (remaining >= businessCount) {
		for (let i = 0; i < businessCount; i += 1) {
			counts[i] = 1;
		}
		remaining -= businessCount;
	} else {
		for (let i = 0; i < remaining; i += 1) {
			counts[i] = 1;
		}
		return counts;
	}

	let index = 0;
	while (remaining > 0) {
		if (counts[index] < maxCardsPerBusiness) {
			counts[index] += 1;
			remaining -= 1;
		}
		index = (index + 1) % businessCount;
	}
	return counts;
}

function buildBudgetSeed(
	businessIds: string[],
	budgetCounts: number[],
): RowSeed {
	let businessIndex = 0;
	let remaining = budgetCounts[0] ?? 0;
	return () => {
		while (businessIndex < businessIds.length && remaining === 0) {
			businessIndex += 1;
			remaining = budgetCounts[businessIndex] ?? 0;
		}
		const businessId = businessIds[businessIndex];
		if (!businessId) return {};
		remaining -= 1;
		return {
			id: faker.string.uuid(),
			business_uuid: businessId,
			parent_budget_id: null,
		};
	};
}

function buildCardSeed(
	plans: BusinessPlan[],
	fallbackBudgetIds: string[],
): RowSeed {
	let businessIndex = 0;
	let remaining = plans[0]?.cardCount ?? 0;
	let budgetIndex = 0;
	return () => {
		while (businessIndex < plans.length && remaining === 0) {
			businessIndex += 1;
			remaining = plans[businessIndex]?.cardCount ?? 0;
			budgetIndex = 0;
		}
		const plan = plans[businessIndex];
		const budgetPool =
			plan && plan.budgetIds.length > 0 ? plan.budgetIds : fallbackBudgetIds;
		const budgetId =
			budgetPool.length > 0
				? (budgetPool[budgetIndex % budgetPool.length] ?? faker.string.uuid())
				: faker.string.uuid();
		budgetIndex += 1;
		if (remaining > 0) remaining -= 1;
		return {
			id: faker.string.uuid(),
			budget_id: budgetId,
		};
	};
}

async function main(): Promise<void> {
	const args = parseArgs(Bun.argv.slice(2));
	if (!args) {
		printUsage();
		return;
	}

	if (args.seed !== undefined && Number.isFinite(args.seed)) {
		faker.seed(args.seed);
	}

	const context: GenerationContext = {
		businessIds: [],
		budgetIds: [],
		applicationIds: [],
		budgetsByBusiness: new Map(),
	};
	const outDir = resolve(args.outDir);

	let businessCount = args.rows.business;
	let budgetCount = args.rows.budget;
	let cardCount = args.rows.card;

	if (cardCount > 0 && !args.explicitRows.business) {
		businessCount = Math.max(
			1,
			Math.ceil(cardCount / args.maxCardsPerBusiness),
		);
		args.rows.business = businessCount;
	}

	const maxCapacity = businessCount * args.maxCardsPerBusiness;
	if (cardCount > maxCapacity) {
		console.warn(
			`Requested ${cardCount} cards exceeds capacity ${maxCapacity} ` +
				`(${businessCount} businesses * ${args.maxCardsPerBusiness} max). ` +
				`Capping card rows to ${maxCapacity}.`,
		);
		cardCount = maxCapacity;
		args.rows.card = cardCount;
	}

	if (!args.explicitRows.budget) {
		budgetCount = businessCount * DEFAULT_BUDGETS_PER_BUSINESS;
		args.rows.budget = budgetCount;
	}

	if (budgetCount < businessCount) {
		console.warn(
			`Budget rows (${budgetCount}) less than business rows ` +
				`(${businessCount}). Increasing budgets to ${businessCount}.`,
		);
		budgetCount = businessCount;
		args.rows.budget = budgetCount;
	}

	console.log(
		`Plan: businesses=${businessCount}, budgets=${budgetCount}, ` +
			`cards=${cardCount}, maxCardsPerBusiness=${args.maxCardsPerBusiness}`,
	);

	const enumOverrides = await loadEnumOverrides(args.enumFile);

	const businessProfiles = applyEnumOverrides(
		await loadProfiles(resolve(args.inputFiles.business), args.sampleRows),
		"business",
		enumOverrides,
	);
	const budgetProfiles = applyEnumOverrides(
		await loadProfiles(resolve(args.inputFiles.budget), args.sampleRows),
		"budget",
		enumOverrides,
	);
	const cardProfiles = applyEnumOverrides(
		await loadProfiles(resolve(args.inputFiles.card), args.sampleRows),
		"card",
		enumOverrides,
	);

	const businessOut = await generateTable(
		"business",
		businessProfiles,
		businessCount,
		outDir,
		context,
		{ progressEvery: args.progressEvery },
	);

	const budgetCounts = allocateBudgetCounts(
		context.businessIds.length,
		budgetCount,
	);
	const budgetSeed = buildBudgetSeed(context.businessIds, budgetCounts);
	const budgetOut = await generateTable(
		"budget",
		budgetProfiles,
		budgetCount,
		outDir,
		context,
		{ rowSeed: budgetSeed, progressEvery: args.progressEvery },
	);

	const cardCounts = allocateCardCounts(
		context.businessIds.length,
		cardCount,
		args.maxCardsPerBusiness,
	);
	const businessPlans: BusinessPlan[] = context.businessIds.map(
		(id, index) => ({
			id,
			budgetIds: context.budgetsByBusiness.get(id) ?? [],
			cardCount: cardCounts[index] ?? 0,
		}),
	);
	const plannedCards = businessPlans.reduce(
		(total, plan) => total + plan.cardCount,
		0,
	);
	if (plannedCards !== cardCount) {
		console.warn(
			`Card plan generated ${plannedCards} rows (requested ${cardCount}).`,
		);
		cardCount = plannedCards;
		args.rows.card = cardCount;
	}
	const cardSeed = buildCardSeed(businessPlans, context.budgetIds);
	const cardOut = await generateTable(
		"card",
		cardProfiles,
		cardCount,
		outDir,
		context,
		{ rowSeed: cardSeed, progressEvery: args.progressEvery },
	);

	const sqlFile = join(outDir, "load.sql");
	const sqlBody = [
		buildCopyStatement("business", businessOut, args.copyMode),
		buildCopyStatement("budget", budgetOut, args.copyMode),
		buildCopyStatement("card", cardOut, args.copyMode),
	].join("\n");
	writeFileSync(sqlFile, `${sqlBody}\n`, "utf8");

	console.log("Generated files:");
	console.log(`- ${businessOut}`);
	console.log(`- ${budgetOut}`);
	console.log(`- ${cardOut}`);
	console.log(`- ${sqlFile}`);
}

main().catch((error) => {
	console.error("Failed to generate data:");
	console.error(error);
	process.exit(1);
});
