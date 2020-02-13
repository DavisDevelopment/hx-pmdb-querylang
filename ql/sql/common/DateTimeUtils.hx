package ql.sql.common;

import ql.sql.common.DateTime;

using ql.sql.common.DateTimeUtils;

/**
* Utility functions for DateTime
*
*/
@:allow(ql.sql.common)
@:access(ql.sql.common)
class DateTimeUtils {
    /**
    * Parse string into DateTime
    *
    */
    static private function fromString (str:String) : DateTime {
        //'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'
        if (str.length == 10 || str.fastCodeAt(10) == ' '.code) {
            return parse(str);

        //'YYYY-MM-DDThh:mm:ss[.SSS]Z'
        } else if (str.fastCodeAt(10) == 'T'.code) {
            return fromIsoString(str);

        //unknown format
        } else {
            throw new pm.Error('`$str` - incorrect date/time format. Should be either `YYYY-MM-DD hh:mm:ss` or `YYYY-MM-DD` or `YYYY-MM-DDThh:mm:ss[.SSS]Z`');
        }
    }//function fromString()


    /**
    * Parse string to DateTime
    *
    */
    static private function parse(str: String):DateTime {
        var ylength : Int = str.indexOf('-');

        if (ylength < 1 || (str.length - ylength != 6 && str.length - ylength != 15)) {
            throw new pm.Error('`$str` - incorrect date/time format. Should be either `YYYY-MM-DD hh:mm:ss` or `YYYY-MM-DD`');
        }

        if (str.length - ylength == 6) {
            str += ' 00:00:00';
        }

        // YYYY-MM-DD hh:mm:ss
        var year    : Null<Int> = Std.parseInt(str.substr(0, ylength));
        var month   : Null<Int> = Std.parseInt(str.substr(ylength + 1, 2));
        var day     : Null<Int> = Std.parseInt(str.substr(ylength + 4, 2));
        var hour    : Null<Int> = Std.parseInt(str.substr(ylength + 7, 2));
        var minute  : Null<Int> = Std.parseInt(str.substr(ylength + 10, 2));
        var second  : Null<Int> = Std.parseInt(str.substr(ylength + 13, 2));

        if (year == null || month == null || day == null || hour == null || minute == null || second == null) {
            throw new pm.Error('`$str` - incorrect date/time format. Should be either `YYYY-MM-DD hh:mm:ss` or `YYYY-MM-DD`');
        }

        return DateTime.make(year, month, day, hour, minute, second);
    }//function parse()


    /**
    * Parse iso string into DateTime
    *
    */
    static private function fromIsoString (str: String):DateTime {
        var dotPos : Int = str.indexOf('.');
        var zPos   : Int = str.indexOf('Z');

        if (str.fastCodeAt(str.length - 1) != 'Z'.code) {
            throw new pm.Error('`$str` - incorrect date/time format. Not an ISO 8601 UTC/Zulu string: Z not found.');
        }

        if (str.length > 20) {
            if (str.fastCodeAt(19) != '.'.code) {
                throw new pm.Error('`$str` - incorrect date/time format. Not an ISO 8601 string: Millisecond specification erroneous.');
            }
            if (str.fastCodeAt(23) != 'Z'.code) {
                throw new pm.Error('`$str` - incorrect date/time format. Not an ISO 8601 string: Timezone specification erroneous.');
            }
        }

        return parse(str.substr(0, 10) + ' ' + str.substr(11, 19 - 11));
    }//function fromIsoString()


    /**
    * Make sure `value` is not less than `min` and not greater than `max`
    *
    */
    static private inline function clamp<T:Float> (value:T, min:T, max:T) : T {
        return (value < min ? min : (value > max ? max : value));
    }//function clamp()


    /**
    * Convert year number (4 digits) to DateTime-timestamp (seconds since 1 a.d.)
    *
    */
    static private function yearToStamp (year:Int) : Float {
        year --;
        var cquads     : Int = Std.int(year / 400);
        var quads      : Int = Std.int((year - cquads * 400) / 4);
        var excessDays : Int = Std.int(quads / 25); //non-leap centuries


        return cquads * DateTime.SECONDS_IN_CQUAD + quads * DateTime.SECONDS_IN_QUAD - excessDays * DateTime.SECONDS_IN_DAY + (year - cquads * 400 - quads * 4) * DateTime.SECONDS_IN_YEAR;
    }//function yearToStamp()


    /**
    * Add specified amount of years to `dt`.
    * Returns unix timestamp.
    */
    static private function addYear (dt:DateTime, amount:Int) : Float {
        var year : Int = dt.getYear() + amount;
        var time : Float = dt.getTime() - (dt.yearStart() + dt.getMonth().toSeconds( dt.isLeapYear() ));

        return yearToStamp(year)
                + dt.getMonth().toSeconds(DateTime.isLeap(year))
                + time
                - DateTime.UNIX_EPOCH_DIFF;
    }//function addYear()


    /**
    * Add specified amount of years to `dt`
    *
    */
    static private function addMonth (dt:DateTime, amount:Int) : Float {
        var month : Int = dt.getMonth() + amount;

        if (month >= 12) {
            var years : Int = Std.int(month / 12);
            dt = addYear(dt, years);
            month -= years * 12;
        } else if (month <= 0) {
            var years : Int = Std.int(month / 12) - 1;
            dt = addYear(dt, years);
            month -= years * 12;
        }

        var isLeap : Bool = dt.isLeapYear();
        var day    : Int  = clamp(dt.getDay(), 1, month.days(isLeap));

        return dt.yearStart()
                + month.toSeconds(isLeap)
                + (day - 1) * DateTime.SECONDS_IN_DAY
                + dt.getHour() * DateTime.SECONDS_IN_HOUR
                + dt.getMinute() * DateTime.SECONDS_IN_MINUTE
                + dt.getSecond();
    }//function addMonth()


    /**
    * Get unix timestamp of a specified `weekDay` in this month, which is the `num`st in current month.
    *
    */
    static private function getWeekDayNum (dt:DateTime, weekDay:Int, num:Int) : Float {
        var month : Int = dt.getMonth();

        if (num > 0) {
            var start : DateTime = dt.monthStart(month) - 1;
            var first : DateTime = start.snap(Week(Up, weekDay));

            return (first + Week(num - 1)).getTime();

        } else if (num < 0) {
            var start : DateTime = dt.monthStart(month + 1) - 1;
            var first : DateTime = start.snap(Week(Down, weekDay));

            return (first + Week(num + 1)).getTime();

        } else {
            return dt.getTime();
        }
    }//function getWeekDayNum()


    /**
    * Limited strftime implementation
    *
    */
    static private function strftime (dt:DateTime, format:String) : String {
        var prevPos : Int = 0;
        var pos     : Int    = format.indexOf('%');
        var str     : String = '';

        while (pos >= 0) {
            str += format.substring(prevPos, pos);
            pos ++;

            switch (format.fastCodeAt(pos)) {
                // %d  Two-digit day of the month (with leading zeros) 01 to 31
                case 'd'.code:
                    str += (dt.getDay() + '').lpad('0', 2);
                // %e  Day of the month, with a space preceding single digits.  1 to 31
                case 'e'.code:
                    str += (dt.getDay() + '').lpad(' ', 2);
                // %j  Day of the year, 3 digits with leading zeros    001 to 366
                case 'j'.code:
                    var day : Int = Std.int( (dt.getTime() - dt.yearStart()) / DateTime.SECONDS_IN_DAY ) + 1;
                    str += '$day'.lpad('0', 3);
                // %u  ISO-8601 numeric representation of the day of the week  1 (for Monday) though 7 (for Sunday)
                case 'u'.code:
                    str += dt.getWeekDay(true) + '';
                // %w  Numeric representation of the day of the week   0 (for Sunday) through 6 (for Saturday)
                case 'w'.code:
                    str += dt.getWeekDay() + '';
                // %m  Two digit representation of the month   01 (for January) through 12 (for December)
                case 'm'.code:
                    str += (dt.getMonth() + '').lpad('0', 2);
                // %C  Two digit representation of the century (year divided by 100, truncated to an integer)  19 for the 20th Century
                case 'C'.code:
                    str += (Std.int(dt.getYear() / 100) + '').lpad('0', 2);
                // %y  Two digit representation of the year    Example: 09 for 2009, 79 for 1979
                case 'y'.code:
                    str += (dt.getYear() + '').substr(-2).lpad('0', 2);
                // %Y  Four digit representation for the year  Example: 2038
                case 'Y'.code:
                    str += dt.getYear() + '';
                // %V  ISO-8601:1988 week number of the given year, starting with the first week of the year with at least 4 weekdays
                case 'V'.code:
                    str += (dt.getWeek() + '').lpad('0', 2);
                // %H  Two digit representation of the hour in 24-hour format  00 through 23
                case 'H'.code:
                    str += (dt.getHour() + '').lpad('0', 2);
                // %k  Two digit representation of the hour in 24-hour format, with a space preceding single digits    0 through 23
                case 'k'.code:
                    str += (dt.getHour() + '').lpad(' ', 2);
                // %I  Two digit representation of the hour in 12-hour format  01 through 12
                case 'I'.code:
                    str += (dt.getHour12() + '').lpad('0', 2);
                // %l  (lower-case 'L') Hour in 12-hour format, with a space preceding single digits    1 through 12
                case 'l'.code:
                    str += (dt.getHour12() + '').lpad(' ', 2);
                // %M  Two digit representation of the minute  00 through 59
                case 'M'.code:
                    str += (dt.getMinute() + '').lpad('0', 2);
                // %p  UPPER-CASE 'AM' or 'PM' based on the given time Example: AM for 00:31, PM for 22:23
                case 'p'.code:
                    str += (dt.getHour() < 12 ? 'AM' : 'PM');
                // %P  lower-case 'am' or 'pm' based on the given time Example: am for 00:31, pm for 22:23
                case 'P'.code:
                    str += (dt.getHour() < 12 ? 'am' : 'pm');
                // %r  Same as "%I:%M:%S %p"   Example: 09:34:17 PM for 21:34:17
                case 'r'.code:
                    str += (dt.getHour12() + ':').lpad('0', 3) + (dt.getMinute() + ':').lpad('0', 3) + (dt.getSecond() + '').lpad('0', 2);
                // %R  Same as "%H:%M" Example: 00:35 for 12:35 AM, 16:44 for 4:44 PM
                case 'R'.code:
                    str += (dt.getHour() + ':').lpad('0', 3) + (dt.getMinute() + '').lpad('0', 2);
                // %S  Two digit representation of the second  00 through 59
                case 'S'.code:
                    str += (dt.getSecond() + '').lpad('0', 2);
                // %T  Same as "%H:%M:%S"  Example: 21:34:17 for 09:34:17 PM
                case 'T'.code:
                    str += (dt.getHour() + ':').lpad('0', 3) + (dt.getMinute() + ':').lpad('0', 3) + (dt.getSecond() + '').lpad('0', 2);
                // %D  Same as "%m/%d/%y"  Example: 02/05/09 for February 5, 2009
                case 'D'.code:
                    str += (dt.getMonth() + '/').lpad('0', 3) + (dt.getDay() + '/').lpad('0', 3) + (dt.getYear() + '').substr(-2).lpad('0', 2);
                // %F  Same as "%Y-%m-%d" (commonly used in database datestamps)   Example: 2009-02-05 for February 5, 2009
                case 'F'.code:
                    str += dt.getYear() + '-' + (dt.getMonth() + '-').lpad('0', 3) + (dt.getDay() + '').lpad('0', 2);
                // %s  Unix Epoch Time timestamp Example: 305815200 for September 10, 1979 08:40:00 AM
                case 's'.code:
                    str += dt.getTime() + '';
                // %%  A literal percentage character ("%")
                case '%'.code:
                    str += '%';
            }//switch()

            prevPos = pos + 1;
            pos = format.indexOf('%', pos + 1);
        }
        str += format.substring(prevPos);

        return str;
    }//function strftime()


    /**
    * Instantiating is not allowed
    *
    */
    private function new () : Void {
    }//function new()



}//class DateTimeUtils

class DateTimeMonthUtils {
	/**
	 * Returns amount of days in specified month (1-12)
	 *
	 */
	static private function days(month:Int, isLeapYear:Bool = false):Int {
		return if (month == 1) 31 // Jan
		else if (month == 2 && isLeapYear) 29 // Feb, leap year
		else if (month == 2) 28 // Feb, normal year
		else if (month == 3) 31 // Mar
		else if (month == 4) 30 // Apr
		else if (month == 5) 31 // May
		else if (month == 6) 30 // Jun
		else if (month == 7) 31 // Jul
		else if (month == 8) 31 // Aug
		else if (month == 9) 30 // Sep
		else if (month == 10) 31 // Oct
		else if (month == 11) 30 // Nov
		else 31 // Dec
		;
	} // function days()

	/**
	 * Get month number based on number of `days` passed since start of a year
	 *
	 */
	static private function getMonth(days:Int, isLeapYear:Bool = false):Int {
		if (days < 32)
			return 1 // Jan
		else if (isLeapYear) {
			if (days < 61)
				return 2 // Feb
			else if (days < 92)
				return 3 // Mar
			else if (days < 122)
				return 4 // Apr
			else if (days < 153)
				return 5 // May
			else if (days < 183)
				return 6 // Jun
			else if (days < 214)
				return 7 // Jul
			else if (days < 245)
				return 8 // Aug
			else if (days < 275)
				return 9 // Sep
			else if (days < 306)
				return 10 // Oct
			else if (days < 336)
				return 11 // Nov
			else
				return 12; // Dec
		} else {
			if (days < 60)
				return 2 // Feb
			else if (days < 91)
				return 3 // Mar
			else if (days < 121)
				return 4 // Apr
			else if (days < 152)
				return 5 // May
			else if (days < 182)
				return 6 // Jun
			else if (days < 213)
				return 7 // Jul
			else if (days < 244)
				return 8 // Aug
			else if (days < 274)
				return 9 // Sep
			else if (days < 305)
				return 10 // Oct
			else if (days < 335)
				return 11 // Nov
			else
				return 12; // Dec
		}
	} // function getMonth()

	/**
	 * Get day number (1-31) based on number of `days` passed since start of a year
	 *
	 */
	static private function getMonthDay(days:Int, isLeapYear:Bool = false):Int {
		if (days < 32)
			return days // Jan
		else if (isLeapYear) {
			if (days < 61)
				return days - 31 // Feb
			else if (days < 92)
				return days - 60 // Mar
			else if (days < 122)
				return days - 91 // Apr
			else if (days < 153)
				return days - 121 // May
			else if (days < 183)
				return days - 152 // Jun
			else if (days < 214)
				return days - 182 // Jul
			else if (days < 245)
				return days - 213 // Aug
			else if (days < 275)
				return days - 244 // Sep
			else if (days < 306)
				return days - 274 // Oct
			else if (days < 336)
				return days - 305 // Nov
			else
				return days - 335; // Dec
		} else {
			if (days < 60)
				return days - 31 // Feb
			else if (days < 91)
				return days - 59 // Mar
			else if (days < 121)
				return days - 90 // Apr
			else if (days < 152)
				return days - 120 // May
			else if (days < 182)
				return days - 151 // Jun
			else if (days < 213)
				return days - 181 // Jul
			else if (days < 244)
				return days - 212 // Aug
			else if (days < 274)
				return days - 243 // Sep
			else if (days < 305)
				return days - 273 // Oct
			else if (days < 335)
				return days - 304 // Nov
			else
				return days - 334; // Dec
		}
	} // function getMonthDay()

	/**
	 * Convert month number to amount of seconds passed since year start
	 *
	 */
	static private function toSeconds(month:Int, isLeapYear:Bool = false):Int {
		return DateTime.SECONDS_IN_DAY * if (month == 1) 0 // Jan
		else if (isLeapYear) {
			if (month == 2)
				31 // Feb
			else if (month == 3)
				60 // Mar
			else if (month == 4)
				91 // Apr
			else if (month == 5)
				121 // May
			else if (month == 6)
				152 // Jun
			else if (month == 7)
				182 // Jul
			else if (month == 8)
				213 // Aug
			else if (month == 9)
				244 // Sep
			else if (month == 10)
				274 // Oct
			else if (month == 11)
				305 // Nov
			else
				335; // Dec
		} else {
			if (month == 2)
				31 // Feb
			else if (month == 3)
				59 // Mar
			else if (month == 4)
				90 // Apr
			else if (month == 5)
				120 // May
			else if (month == 6)
				151 // Jun
			else if (month == 7)
				181 // Jul
			else if (month == 8)
				212 // Aug
			else if (month == 9)
				243 // Sep
			else if (month == 10)
				273 // Oct
			else if (month == 11)
				304 // Nov
			else
				334; // Dec
		};
	}

	/**
	 * Instantiating is not allowed
	 *
	 */
	private function new():Void {}

}

/**
 * Snap implementations
 *
 */
@:allow(ql.sql.common)
@:access(ql.sql.common)
class DateTimeSnapUtils {
	/**
	 * Snap to nearest year.
	 * Returns unix timestamp.
	 */
	static private function snapYear(dt:DateTime, direction:DTSnapDirection):Float {
		switch (direction) {
			case Down:
				return dt.yearStart();

			case Up:
				var next:DateTime = dt.addYear(1);
				return next.yearStart();

			case Nearest:
				var next:Float = new DateTime(dt.addYear(1)).yearStart();
				var previous:Float = dt.yearStart();

				return (next - dt.getTime() > dt.getTime() - previous ? previous : next);
		}
	} // function snapYear()

	/**
	 * Snap to nearest month
	 * Returns unix timestamp
	 */
	static private function snapMonth(dt:DateTime, direction:DTSnapDirection):Float {
		var month:Int = dt.getMonth();
		var isLeap:Bool = dt.isLeapYear();

		switch (direction) {
			case Down:
				return dt.yearStart() + month.toSeconds(isLeap);

			case Up:
				return dt.yearStart() + month.toSeconds(isLeap) + month.days(isLeap) * DateTime.SECONDS_IN_DAY;

			case Nearest:
				var previous = dt.yearStart() + month.toSeconds(isLeap);
				var next = dt.yearStart() + month.toSeconds(isLeap) + month.days(isLeap) * DateTime.SECONDS_IN_DAY;

				return (next - dt.getTime() > dt.getTime() - previous ? previous : next);
		}
	} // function snapMonth()

	/**
	 * Snap to nearest day
	 * Returns unix timestamp
	 */
	static private function snapDay(dt:DateTime, direction:DTSnapDirection):Float {
		var days:Float = dt.getTime() / DateTime.SECONDS_IN_DAY;

		return switch (direction) {
			case Down: Math.ffloor(days) * DateTime.SECONDS_IN_DAY;
			case Up: Math.fceil(days) * DateTime.SECONDS_IN_DAY;
			case Nearest: Math.fround(days) * DateTime.SECONDS_IN_DAY;
		}
	} // function snapDay()

	/**
	 * Snap to nearest hour
	 * Returns unix timestamp
	 */
	static private function snapHour(dt:DateTime, direction:DTSnapDirection):Float {
		var hours:Float = dt.getTime() / DateTime.SECONDS_IN_HOUR;

		return switch (direction) {
			case Down: Math.ffloor(hours) * DateTime.SECONDS_IN_HOUR;
			case Up: Math.fceil(hours) * DateTime.SECONDS_IN_HOUR;
			case Nearest: Math.fround(hours) * DateTime.SECONDS_IN_HOUR;
		}
	} // function snapHour()

	/**
	 * Snap to nearest minute
	 * Returns unix timestamp
	 */
	static private function snapMinute(dt:DateTime, direction:DTSnapDirection):Float {
		var minutes:Float = dt.getTime() / DateTime.SECONDS_IN_MINUTE;

		return switch (direction) {
			case Down: Math.ffloor(minutes) * DateTime.SECONDS_IN_MINUTE;
			case Up: Math.fceil(minutes) * DateTime.SECONDS_IN_MINUTE;
			case Nearest: Math.fround(minutes) * DateTime.SECONDS_IN_MINUTE;
		}
	} // function snapMinute()

	/**
	 * Snap to nearest `required` week day
	 * Returns unix timestamp
	 */
	static private function snapWeek(dt:DateTime, direction:DTSnapDirection, required:Int):Float {
		var current:Int = dt.getWeekDay();

		var days:Float = Math.ffloor(dt.getTime() / DateTime.SECONDS_IN_DAY);

		switch (direction) {
			case Down:
				var diff:Int = (current >= required ? current - required : current + 7 - required);
				return (days - diff) * DateTime.SECONDS_IN_DAY;

			case Up:
				var diff:Int = (required > current ? required - current : required + 7 - current);
				return (days + diff) * DateTime.SECONDS_IN_DAY;

			case Nearest:
				var diff:Int = (current >= required ? current - required : current + 7 - required);
				var previous:Float = (days - diff) * DateTime.SECONDS_IN_DAY;

				var diff:Int = (required > current ? required - current : required + 7 - current);
				var next:Float = (days + diff) * DateTime.SECONDS_IN_DAY;

				return (next - dt.getTime() > dt.getTime() - previous ? previous : next);
		}
	} // function snapWeek()

	/**
	 * Instantiating is not allowed
	 *
	 */
	private function new():Void {} // function new()

} // class DateTimeSnapUtils