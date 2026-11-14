package query

// The function surface is a closed, compile-time allowlist (design §7.2). It
// is deliberately not derived from DuckDB's runtime function catalog, so a
// DuckDB upgrade that adds functions cannot widen the surface. Names are the
// lower-cased names DuckDB's parser emits after alias resolution of operators
// and syntax sugar.

func set(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// operatorNames are parser-level operators (is_operator=true).
var operatorNames = set(
	"+", "-", "*", "/", "//", "%", "^", "||",
	"~~", "!~~", "~~*", "!~~*", // LIKE, NOT LIKE, ILIKE, NOT ILIKE
	"~", "!~", // regex via SIMILAR TO / ~
	"array_extract", "array_slice", "struct_extract", "list_extract", "map_extract", "map_extract_value",
	"not", "!",
)

// internalFunctions are names the parser synthesizes for allowed syntax.
var internalFunctions = set(
	"count_star", "like_escape", "not_like_escape", "ilike_escape", "not_ilike_escape",
	"similar_to_escape", "not_similar_to_escape",
	"array_extract", "array_slice", "struct_extract", "list_extract", "map_extract", "map_extract_value",
	"unnest", "date_part", "typeof", "try_cast", "list_value",
	"regexp_full_match", "regexp_matches", "regexp_replace", "regexp_extract",
	"row_number",
)

var scalarFunctions = set(
	// string
	"lower", "upper", "trim", "ltrim", "rtrim", "length", "len", "substring", "substr",
	"concat", "concat_ws", "replace", "reverse", "split", "string_split", "str_split", "string_to_array",
	"position", "strpos", "left", "right", "lpad", "rpad", "repeat", "starts_with", "ends_with",
	"contains", "prefix", "suffix", "regexp_matches", "regexp_replace", "regexp_extract", "regexp_full_match",
	// numeric
	"abs", "sign", "round", "floor", "ceil", "ceiling", "sqrt", "pow", "power", "exp", "ln", "log",
	"log2", "log10", "mod", "greatest", "least",
	// date/time
	"current_date", "current_time", "current_timestamp", "now", "date_trunc", "datetrunc", "date_part",
	"datepart", "date_diff", "datediff", "date_add", "dateadd", "date_sub", "datesub", "extract", "strftime",
	"make_date", "make_time", "make_timestamp", "epoch", "to_timestamp", "year", "month", "day", "hour",
	"minute", "second", "dayofmonth", "dayofweek", "dayofyear", "week", "quarter",
	// conditional
	"coalesce", "nullif", "ifnull", "if",
	// nested accessors
	"struct_extract", "list_extract", "array_extract", "map_extract", "map_extract_value", "unnest",
	"typeof",
)

var aggregateFunctions = set(
	"count", "count_star", "sum", "avg", "mean", "min", "max", "approx_count_distinct",
	"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop", "median", "mode",
	"quantile_cont", "quantile", "quantile_disc", "arg_min", "min_by", "arg_max", "max_by",
	"string_agg", "group_concat", "list", "array_agg", "first", "last", "any_value",
	"bool_and", "bool_or", "bit_and", "bit_or", "bit_xor",
)

var windowFunctions = set(
	"row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead",
	"first_value", "last_value", "nth_value",
)
