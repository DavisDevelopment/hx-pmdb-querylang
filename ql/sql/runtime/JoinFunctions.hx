package ql.sql.runtime;

import pmdb.core.Object.Doc;

using pm.Iterators;
using pm.Arrays;

@:yield
class JoinFunctions {
    static function yieldRightSubList(sortedList:Array<Doc>, accessor:Doc->Dynamic):Iterator<SubList> {
		if (sortedList.length == 1) {
			@yield return {r: sortedList, val: accessor(sortedList[sortedList.length - 1])};
        } 
        else if (sortedList.length > 1) {
            var i:Int = sortedList.length;
            var r:Array<Dynamic> = [sortedList[--i]];
            var val:Dynamic = accessor(r[0]);
			// for each subsequent value, we'll yield when there is a
			// new tmpVal that is not equal the current val
			while (i-- > 0) {
				final tmpVal = accessor(sortedList[i]);
				if (val <= tmpVal && val >= tmpVal) {
					r.unshift(sortedList[i]);
                } 
                else {
					@yield return {r:r, val:val};
					r = [sortedList[i]];
					val = tmpVal;
				}
			}
			@yield return {r:r, val:val};
		}
    }

    static inline function sortBy<T>(arr:Array<T>, accessor:T -> Dynamic, ?compare:Dynamic->Dynamic->Int) {
        if (compare == null) 
            compare = pmdb.core.Arch.compareThings;
        return arr.sorted(function(a:T, b:T) {
            return compare(accessor(a), accessor(b));
        });
    }

	static function mergeLists(aDatumsR:Array<Dynamic>, bDatumsR:Array<Dynamic>, merger:Dynamic->Dynamic->Dynamic):Array<Dynamic> {
        inline function reduceRight(a:Array<Dynamic>, f, acc) {
            return a.reduceRight(f, acc);
        }

		return reduceRight(aDatumsR, function (previous, datum) {
            return reduceRight(bDatumsR, function (prev:Array<Dynamic>, cDatum) {
			    prev.unshift(merger(datum, cDatum));
			    return prev;
            }, []).concat(previous);
        }, []);
	}

    public static function sortedMergeInnerJoin(a:Array<Doc>, b:Array<Doc>, aAccessor:Doc->Dynamic, bAccessor:Doc->Dynamic, merger:Doc->Doc->Doc):Array<Doc> {
		if (a.length < 1 || b.length < 1) {
			return [];
		}
        var aSorted = sortBy(a, aAccessor);
        var bSorted = sortBy(b, bAccessor);
        var aGenerator = yieldRightSubList(aSorted, aAccessor); 
        var bGenerator = yieldRightSubList(bSorted, bAccessor);
        var r = [];
        var aDatums = aGenerator.next();
        var bDatums = bGenerator.next();

		while (aDatums != null && bDatums != null) {
			if (aDatums.val > bDatums.val) {
				aDatums = aGenerator.next();
            } 
            else if (aDatums.val < bDatums.val) {
				bDatums = bGenerator.next();
            } 
            else {
				r = mergeLists(aDatums.r, bDatums.r, merger).concat(r);
				aDatums = aGenerator.next();
				bDatums = bGenerator.next();
			}
		}
		return r;
    }

	public static function equijoin(arr1:Array<Doc>, arr2:Array<Doc>, arr1Key:Dynamic, arr2Key:Dynamic, select:(a:Doc, b:Doc) -> Doc):Array<Doc> {
		var m = arr1.length,
			n = arr2.length,
			index = new Map(),
			c:Array<Doc> = [],
			row:Doc,
			rowKey,
			arr1Row;

		function mapFunc(keyItem:String) { // Build the composite key
			return row.get(keyItem);
		}

		inline function isArray(x:Dynamic):Bool {
			return (x is Array<Dynamic>);
		}

		if (isArray(arr1Key) && isArray(arr2Key)) {
			var arr1Key:Array<String> = cast arr1Key;
			var arr2Key:Array<String> = cast arr2Key;
			arr1Key.sort(Reflect.compare); /// Allow the key kolumns to be entered in any order.
			arr2Key.sort(Reflect.compare);

			for (i in 0...m) {
				row = arr1[i];
				rowKey = arr1Key.map(mapFunc).join('~~'); /// combine the key values for lookup later
				index.set(rowKey, row); // create an index for arr1 table
			}

			for (j in 0...n) { // loop through n items
				row = arr2[j];
				rowKey = arr2Key.map(mapFunc).join('~~');
				if (rowKey != null && rowKey != '') { /// NULL !== NULL
					arr1Row = index.get(rowKey); // get corresponding row from arr1
				} else {
					arr1Row = null;
				}

				if (arr1Row != null) {
					c.push(select(arr1Row, row)); // select only the columns you need
				}
			}
		} else {
			for (k in 0...m) { // loop through m items
				row = arr1[k];
				index[row.get(arr1Key)] = row; // create an index for arr1 table
			}

			for (l in 0...n) { // loop through n items
				row = arr2[l];
				if (row[arr2Key] != null) {
					arr1Row = index[row[arr2Key]]; // get corresponding row from arr1
				} else {
					arr1Row = null;
				}

				if (arr1Row != null) {
					c.push(select(arr1Row, row)); // select only the columns you need
				}
			}
		}

		return c;
	}

	public static function rightjoin(arr1:Array<Doc>, arr2:Array<Doc>, arr1Key:Dynamic, arr2Key:Dynamic, select:(a:Doc, b:Doc) -> Doc):Array<Doc> {
		var m = arr1.length,
			n = arr2.length,
			index:Map<String, Doc> = new Map(),
			c = [],
			row:Doc,
			rowKey,
			arr1Row;

		function mapFunc(keyItem) { // Build the composite key
			return row[keyItem];
		}
		function isArray(x:Dynamic) {
			return (x is Array<Dynamic>);
		}

		if (isArray(arr1Key) && isArray(arr2Key)) {
			var arr1Key:Array<String> = cast arr1Key;
			var arr2Key:Array<String> = cast arr2Key;
			arr1Key.sort(Reflect.compare); /// Allow the key kolumns to be entered in any order.
			arr2Key.sort(Reflect.compare);
			//       for (var i = 0; i < m; i++) {     // loop through m items
			for (i in 0...m) {
				row = arr1[i];
				rowKey = arr1Key.map(mapFunc).join('~~'); /// combine the key values for lookup later
				index.set(rowKey, row); // create an index for arr1 table
			}

			for (j in 0...n) { // loop through n items
				row = arr2[j];
				rowKey = arr2Key.map(mapFunc).join('~~');

				if (rowKey != null && rowKey != '') { /// NULL !== NULL
					arr1Row = index.get(rowKey); // get corresponding row from arr1
				} else {
					arr1Row = new Doc();
				}

				if (arr1Row == null) {
					arr1Row = new Doc();
				}
				c.push(select(arr1Row, row)); // select only the columns you need
			}
		} else {
			for (k in 0...m) { // loop through m items
				row = cast arr1[k];
				index.set(row.get(arr1Key), row);

				// create an index for arr1 table
			}

			for (l in 0...n) { // loop through n items
				row = arr2[l];
				if (row[arr2Key] != null) { /// NULL !== NULL
					arr1Row = index.get(row[arr2Key]); // get corresponding row from arr1
				} else {
					arr1Row = new Doc();
				}

				if (arr1Row == null) {
					arr1Row = new Doc();
				}
				c.push(select(arr1Row, row)); // select only the columns you need
			}
		}

		return c;
	}
}

typedef SubList = {r:Array<Dynamic>, val:Dynamic};