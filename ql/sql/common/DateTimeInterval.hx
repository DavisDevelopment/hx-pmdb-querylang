package ql.sql.common;

import ql.sql.common.DateTime;

using ql.sql.common.DateTimeUtils;

/**
 * Time interval representation.
 *   Stores difference in seconds between two DateTime instances.
 *   Amounts of years/months/days/hours/minutes/seconds are calculated accounting leap years.
 *   Maximum allowed interval is ~4100 years.
 */
@:allow(ql.sql.common)
@:access(ql.sql.common)
class DateTimeIntervalCore {
	/** Indicates if this is negative interval */
	public var negative(default, null):Bool = false;

	/** DateTime instance of the beginning of this interval */
	private var begin:DateTime;

	/** DateTime instance of the end of this interval */
	private var end:DateTime;

	/** amount of years */
	private var years:Int = -1;

	/** amount of months */
	private var months:Int = -1;

	/** amount of days */
	private var days:Int = -1;

	/** amount of hours */
	private var hours:Int = -1;

	/** amount of minutes */
	private var minutes:Int = -1;

	/** amount of seconds */
	private var seconds:Int = -1;

	/**
	 * Constructor.
	 * Manual instantiation is not allowed.
	 *
	 */
	private function new():Void {
		// code...
	} // function new()

	/**
	 * Get amount of full years in this interval.
	 *
	 */
	public function getYears():Int {
		if (years < 0) {
			years = end.getYear() - begin.getYear();

			var m1 = begin.getMonth();
			var m2 = end.getMonth();
			if (m2 < m1) {
				years--;
			} else if (m1 == m2) {
				var d1 = begin.getDay();
				var d2 = end.getDay();
				if (d2 < d1) {
					years--;
				} else if (d1 == d2) {
					var h1 = begin.getHour();
					var h2 = end.getHour();
					if (h2 < h1) {
						years--;
					} else if (h2 == h1) {
						var m1 = begin.getMinute();
						var m2 = end.getMinute();
						if (m2 < m1) {
							years--;
						} else if (m2 == m1 && end.getSecond() < begin.getSecond()) {
							years--;
						}
					}
				}
			}
		}

		return years;
	} // function getYears()

	/**
	 * Get amount of full months in this interval (always less then 12)
	 *
	 */
	public function getMonths():Int {
		if (months < 0) {
			var monthBegin:Int = begin.getMonth();
			var monthEnd:Int = end.getMonth();

			months = (monthBegin <= monthEnd ? monthEnd - monthBegin : 12 - monthBegin + monthEnd);

			var d1 = begin.getDay();
			var d2 = end.getDay();
			if (d2 < d1) {
				months--;
			} else if (d1 == d2) {
				var h1 = begin.getHour();
				var h2 = end.getHour();
				if (h2 < h1) {
					months--;
				} else if (h2 == h1) {
					var m1 = begin.getMinute();
					var m2 = end.getMinute();
					if (m2 < m1) {
						months--;
					} else if (m2 == m1 && end.getSecond() < begin.getSecond()) {
						months--;
					}
				}
			}
		}

		return months;
	} // function getMonths()

	/**
	 * Get total amount of months in this interval.
	 *   E.g. DateTimeInterval.fromString('(3y,5m)').getTotalMonths() returns 3 * 12 + 5 = 41
	 *
	 */
	public function getTotalMonths():Int {
		return getYears() * 12 + getMonths();
	} // function getTotalMonths()

	/**
	 * Get amount of full days in this interval (always less then 31)
	 *
	 */
	public function getDays():Int {
		if (days < 0) {
			var dayBegin:Int = begin.getDay();
			var dayEnd:Int = end.getDay();

			days = (dayBegin <= dayEnd ? dayEnd - dayBegin : begin.getMonth().days(begin.isLeapYear()) - dayBegin + dayEnd);

			var h1 = begin.getHour();
			var h2 = end.getHour();
			if (h2 < h1) {
				days--;
			} else if (h2 == h1) {
				var m1 = begin.getMinute();
				var m2 = end.getMinute();
				if (m2 < m1) {
					days--;
				} else if (m2 == m1 && end.getSecond() < begin.getSecond()) {
					days--;
				}
			}
		}

		return days;
	} // function getDays()

	/**
	 * Get total amount of days in this interval.
	 *
	 */
	public function getTotalDays():Int {
		return Std.int((end.getTime() - begin.getTime()) / DateTime.SECONDS_IN_DAY);
	} // function getTotalDays()

	/**
	 * Get amount of full hours in this interval (always less then 24)
	 *
	 */
	public function getHours():Int {
		if (hours < 0) {
			var hourBegin:Int = begin.getHour();
			var hourEnd:Int = end.getHour();

			hours = (hourBegin <= hourEnd ? hourEnd - hourBegin : 24 - hourBegin + hourEnd);

			var m1 = begin.getMinute();
			var m2 = end.getMinute();
			if (m2 < m1) {
				hours--;
			} else if (m2 == m1 && end.getSecond() < begin.getSecond()) {
				hours--;
			}
		}

		return hours;
	} // function getHours()

	/**
	 * Get total amount of hours in this interval.
	 *
	 */
	public function getTotalHours():Int {
		return Std.int((end.getTime() - begin.getTime()) / DateTime.SECONDS_IN_HOUR);
	} // function getTotalHours()

	/**
	 * Get amount of full minutes in this interval (always less then 60)
	 *
	 */
	public function getMinutes():Int {
		if (minutes < 0) {
			var minuteBegin:Int = begin.getMinute();
			var minuteEnd:Int = end.getMinute();

			minutes = (minuteBegin <= minuteEnd ? minuteEnd - minuteBegin : 60 - minuteBegin + minuteEnd);

			if (end.getSecond() < begin.getSecond()) {
				minutes--;
			}
		}

		return minutes;
	} // function getMinutes()

	/**
	 * Get total amount of minutes in this interval.
	 *
	 */
	public function getTotalMinutes():Int {
		return Std.int((end.getTime() - begin.getTime()) / DateTime.SECONDS_IN_MINUTE);
	} // function getTotalMinutes()

	/**
	 * Get amount of full seconds in this interval (always less then 60)
	 *
	 */
	public function getSeconds():Int {
		if (seconds < 0) {
			var secondBegin:Int = begin.getSecond();
			var secondEnd:Int = end.getSecond();

			seconds = (secondBegin <= secondEnd ? secondEnd - secondBegin : 60 - secondBegin + secondEnd);
		}

		return seconds;
	} // function getSeconds()

	/**
	 * Get total amount of seconds in this interval.
	 *
	 */
	public function getTotalSeconds():Float {
		return end.getTime() - begin.getTime();
	} // function getTotalSeconds()

	/**
	 * Get total amount of weeks in this interval.
	 *   Not calendar weeks, but each 7 days.
	 */
	public function getTotalWeeks():Int {
		return Std.int((end.getTime() - begin.getTime()) / DateTime.SECONDS_IN_WEEK);
	} // function getTotalWeeks()

} // class DateTimeIntervalCore

@:allow(ql.sql.common)
@:access(ql.sql.common)
@:forward(negative,getYears,getMonths,getDays,getHours,getMinutes,getSeconds,getTotalMonths,getTotalDays,getTotalHours,getTotalMinutes,getTotalSeconds,getTotalWeeks)
abstract DateTimeInterval (DateTimeIntervalCore) to DateTimeIntervalCore from DateTimeIntervalCore {
    /**
     * Create interval as difference between two DateTime instances
     * @param begin 
     * @param end 
     * @return DateTimeInterval
     */
    static public function create(begin:DateTime, end:DateTime):DateTimeInterval {
        var dtic = new DateTimeIntervalCore();
        dtic.begin    = (end < begin ? end : begin);
        dtic.end      = (end < begin ? begin : end);
        dtic.negative = (end < begin);

        return dtic;
    }


    /**
    * Constructor.
    *
    */
    public inline function new (dtic:DateTimeIntervalCore) : Void {
        this = dtic;
    }


    /**
    * Invert the sign of this interval. Modifies internal state. Returns itself.
    *
    */
    public inline function invert () : DateTimeInterval {
        this.negative = !this.negative;
        return this;
    }


    /**
    * Add this interval to specified DateTime instance.
    *
    * Returns new DateTime.
    */
    public function addTo (dt:DateTime) : DateTime {
        return dt.getTime() + sign() * (this.end.getTime() - this.begin.getTime());
    }


    /**
    * Substract this interval from specified DateTime instance.
    *
    * Returns new DateTime.
    */
    public function subFrom (dt:DateTime) : DateTime {
        return dt.getTime() - sign() * (this.end.getTime() - this.begin.getTime());
    }


    /**
    * Get string representation of this interval.
    *
    */
    public function toString () : String {
        var years   = this.getYears();
        var months  = this.getMonths();
        var days    = this.getDays();
        var hours   = this.getHours();
        var minutes = this.getMinutes();
        var seconds = this.getSeconds();

        var parts : Array<String> = [];
        if (years != 0)     parts.push('${years}y');
        if (months != 0)    parts.push('${months}m');
        if (days != 0)      parts.push('${days}d');
        if (hours != 0)     parts.push('${hours}hrs');
        if (minutes != 0)   parts.push('${minutes}min');
        if (seconds != 0)   parts.push('${seconds}sec');

        return (this.negative ? '-' : '') + '(' + (parts.length == 0 ? '0sec' : parts.join(', ')) + ')';
    }


    /**
    *  Returns -1 if this is a negative interval, +1 otherwise
    *
    */
    public inline function sign () : Int {
        return (this.negative ? -1 : 1);
    }


    /**
    * Formats the interval
    *
    *   - `%%` Literal %. Example:   %
    *   - `%Y` Years, numeric, at least 2 digits with leading 0. Example:    01, 03
    *   - `%y` Years, numeric. Example:  1, 3
    *   - `%M` Months, numeric, at least 2 digits with leading 0. Example:   01, 03, 12
    *   - `%m` Months, numeric. Example: 1, 3, 12
    *   - `%b` Total number of months. Example:   2, 15, 36
    *   - `%D` Days, numeric, at least 2 digits with leading 0. Example: 01, 03, 31
    *   - `%d` Days, numeric. Example:   1, 3, 31
    *   - `%a` Total number of days. Example:   4, 18, 8123
    *   - `%H` Hours, numeric, at least 2 digits with leading 0. Example:    01, 03, 23
    *   - `%h` Hours, numeric. Example:  1, 3, 23
    *   - `%c` Total number of hours. Example:   4, 18, 8123
    *   - `%I` Minutes, numeric, at least 2 digits with leading 0. Example:  01, 03, 59
    *   - `%i` Minutes, numeric. Example:    1, 3, 59
    *   - `%e` Total number of minutes. Example:   4, 18, 8123
    *   - `%S` Seconds, numeric, at least 2 digits with leading 0. Example:  01, 03, 57
    *   - `%s` Seconds, numeric. Example:    1, 3, 57
    *   - `%f` Total number of seconds. Example:   4, 18, 8123
    *   - `%R` Sign "-" when negative, "+" when positive. Example:   -, +
    *   - `%r` Sign "-" when negative, empty when positive. Example: -,
    */
    public inline function format (format:String) : String {
        return DateTimeIntervalUtils.strftime(this, format);
    }


    /**
    * Formats  each string in `format` array. Each string can have only one placeholder.
    *
    * Supported placeholders: see `format()` method description except `r,R,%` placeholders.
    *
    * Returns new array with elements, whose corresponding strings in `format` array were filled with non-zero values.
    *
    * Example: if interval contains 0 years, 2 months and 10 days, then
    * `interval.format(['%y years', '%m months', '%d days']).join(',')` will return `'2 months, 10 days'`
    *
    */
    public inline function formatPartial (format:Array<String>) : Array<String> {
        return DateTimeIntervalUtils.formatPartial(this, format);
    }


    /**
    * DateTimeInterval comparison
    *
    */
    @:op(A == B) private inline function eq (dtic:DateTimeInterval) {
        return this.negative == dtic.negative
            && this.getTotalSeconds() == dtic.getTotalSeconds();
    }

    @:op(A > B) private inline function gt (dtic:DateTimeInterval) {
        if (this.negative != dtic.negative) return dtic.negative;

        var delta = this.getTotalSeconds() - dtic.getTotalSeconds();
        return this.negative ? delta < 0 : delta > 0;
    }

    @:op(A >= B) private inline function gte (dtic:DateTimeInterval) return eq(dtic) || gt(dtic);
    @:op(A < B)  private inline function lt (dtic:DateTimeInterval)  return !gte(dtic);
    @:op(A <= B) private inline function lte (dtic:DateTimeInterval) return !gt(dtic);
    @:op(A != B) private inline function neq (dtic:DateTimeInterval) return !eq(dtic);
}

@:allow(ql.sql.common)
@:access(ql.sql.common)
class DateTimeIntervalUtils {
	static private function strftime(dti:DateTimeInterval, format:String):String {
		var prevPos:Int = 0;
		var pos:Int = format.indexOf('%');
		var str:String = '';

		while (pos >= 0) {
			str += format.substring(prevPos, pos);
			pos++;

			switch (format.fastCodeAt(pos)) {
				// Y - Years, numeric, at least 2 digits with leading 0. Example:    01, 03
				case 'Y'.code:
					str += (dti.getYears() + '').lpad('0', 2);
				// y - Years, numeric. Example:  1, 3
				case 'y'.code:
					str += dti.getYears() + '';
				// M - Months, numeric, at least 2 digits with leading 0. Example:   01, 03, 12
				case 'M'.code:
					str += (dti.getMonths() + '').lpad('0', 2);
				// m - Months, numeric. Example: 1, 3, 12
				case 'm'.code:
					str += dti.getMonths() + '';
				// b - Total number of months. Example:   2, 15, 36
				case 'b'.code:
					str += dti.getTotalMonths() + '';
				// D - Days, numeric, at least 2 digits with leading 0. Example: 01, 03, 31
				case 'D'.code:
					str += (dti.getDays() + '').lpad('0', 2);
				// d - Days, numeric. Example:   1, 3, 31
				case 'd'.code:
					str += dti.getDays() + '';
				// a - Total number of days. Example:   4, 18, 8123
				case 'a'.code:
					str += dti.getTotalDays() + '';
				// H - Hours, numeric, at least 2 digits with leading 0. Example:    01, 03, 23
				case 'H'.code:
					str += (dti.getHours() + '').lpad('0', 2);
				// h - Hours, numeric. Example:  1, 3, 23
				case 'h'.code:
					str += dti.getHours() + '';
				// c - Total number of hours. Example:   4, 18, 8123
				case 'c'.code:
					str += dti.getTotalHours() + '';
				// I - Minutes, numeric, at least 2 digits with leading 0. Example:  01, 03, 59
				case 'I'.code:
					str += (dti.getMinutes() + '').lpad('0', 2);
				// i - Minutes, numeric. Example:    1, 3, 59
				case 'i'.code:
					str += dti.getMinutes() + '';
				// e - Total number of minutes. Example:   4, 18, 8123
				case 'e'.code:
					str += dti.getTotalMinutes() + '';
				// S - Seconds, numeric, at least 2 digits with leading 0. Example:  01, 03, 57
				case 'S'.code:
					str += (dti.getSeconds() + '').lpad('0', 2);
				// s - Seconds, numeric. Example:    1, 3, 57
				case 's'.code:
					str += dti.getSeconds() + '';
				// f - Total number of seconds. Example:   4, 18, 8123
				case 'f'.code:
					str += dti.getTotalSeconds() + '';
				// R - Sign "-" when negative, "+" when positive. Example:   -, +
				case 'R'.code:
					str += (dti.negative ? '-' : '+');
				// r - Sign "-" when negative, empty when positive. Example: -,
				case 'r'.code:
					str += (dti.negative ? '-' : '');
				// %%  A literal percentage character ("%")
				case '%'.code:
					str += '%';
			} // switch()

			prevPos = pos + 1;
			pos = format.indexOf('%', pos + 1);
		}
		str += format.substring(prevPos);

		return str;
	} // function strftime()

	/**
	 * Format each string in `format` but only fill one placeholder in each string.
	 *
	 */
	static private function formatPartial(dti:DateTimeInterval, format:Array<String>):Array<String> {
		var result:Array<String> = [];

		var pos:Int = 0;
        var str:String = '';
        
		for (f in 0...format.length) {
			pos = format[f].indexOf('%');
			if (pos >= 0) {
				switch (format[f].fastCodeAt(pos + 1)) {
					// Y - Years, numeric, at least 2 digits with leading 0. Example:    01, 03
					case 'Y'.code:
						if (dti.getYears() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getYears() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// y - Years, numeric. Example:  1, 3
					case 'y'.code:
						if (dti.getYears() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getYears() + format[f].substring(pos + 2);
					// M - Months, numeric, at least 2 digits with leading 0. Example:   01, 03, 12
					case 'M'.code:
						if (dti.getMonths() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getMonths() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// m - Months, numeric. Example: 1, 3, 12
					case 'm'.code:
						if (dti.getMonths() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getMonths() + format[f].substring(pos + 2);
					// b - Total number of months. Example:   2, 15, 36
					case 'b'.code:
						if (dti.getTotalMonths() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getTotalMonths() + format[f].substring(pos + 2);
					// D - Days, numeric, at least 2 digits with leading 0. Example: 01, 03, 31
					case 'D'.code:
						if (dti.getDays() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getDays() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// d - Days, numeric. Example:   1, 3, 31
					case 'd'.code:
						if (dti.getDays() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getDays() + format[f].substring(pos + 2);
					// a - Total number of days. Example:   4, 18, 8123
					case 'a'.code:
						if (dti.getTotalDays() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getTotalDays() + format[f].substring(pos + 2);
					// H - Hours, numeric, at least 2 digits with leading 0. Example:    01, 03, 23
					case 'H'.code:
						if (dti.getHours() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getHours() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// h - Hours, numeric. Example:  1, 3, 23
					case 'h'.code:
						if (dti.getHours() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getHours() + format[f].substring(pos + 2);
					// c - Total number of hours. Example:   4, 18, 8123
					case 'c'.code:
						if (dti.getTotalHours() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getTotalHours() + format[f].substring(pos + 2);
					// I - Minutes, numeric, at least 2 digits with leading 0. Example:  01, 03, 59
					case 'I'.code:
						if (dti.getMinutes() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getMinutes() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// i - Minutes, numeric. Example:    1, 3, 59
					case 'i'.code:
						if (dti.getMinutes() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getMinutes() + format[f].substring(pos + 2);
					// e - Total number of minutes. Example:   4, 18, 8123
					case 'e'.code:
						if (dti.getTotalMinutes() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getTotalMinutes() + format[f].substring(pos + 2);
					// S - Seconds, numeric, at least 2 digits with leading 0. Example:  01, 03, 57
					case 'S'.code:
						if (dti.getSeconds() == 0)
							continue;
						str = format[f].substring(0, pos) + (dti.getSeconds() + '').lpad('0', 2) + format[f].substring(pos + 2);
					// s - Seconds, numeric. Example:    1, 3, 57
					case 's'.code:
						if (dti.getSeconds() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getSeconds() + format[f].substring(pos + 2);
					// f - Total number of seconds. Example:   4, 18, 8123
					case 'f'.code:
						if (dti.getTotalSeconds() == 0)
							continue;
						str = format[f].substring(0, pos) + dti.getTotalSeconds() + format[f].substring(pos + 2);
					// no proper placeholder found
					case _:
						continue;
				} // switch()

				result.push(str);
			}
		}

		return result;
	} // function formatPartial()
}