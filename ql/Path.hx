package ql;

using StringTools;
using pm.Strings;
using pm.Arrays;

import pm.Helpers.*;

using ql.Path;
using pm.Helpers;

class Tools {
    public static function exec(re:EReg, s:String):Array<String> {
        if (re.match(s)) {
            var n = 0;
            var res = [];
            do {
                var p:Null<String> = null;
                try {
                    p = re.matched(n);
                    n++;
                    if (p == null)
                        break;
                    res.push(p);
                } 
                catch (e: Dynamic) {
                    break;
                }
            }
            while (true);
            return res;
        }
        else {
            return [];
        }
    }

    public static extern inline function b(s:String):Bool {
        #if js
        return untyped !!s;
        #else
        return !s.empty();
        #end
    }

	// resolves . and .. elements in a path array with directory names there
	// must be no slashes or device names (c:\) in the array
	// (so also no leading and trailing slashes - it does not distinguish
	// relative and absolute paths)
	public static function normalizeArray(parts:Array<String>, allowAboveRoot=false) {
		var res = [];
		for (i in  0... parts.length) {
			var p = parts[i];

			// ignore empty parts
			if (p == null || p == '' || p == '.')
				continue;

			if (p == '..') {
				if (res.length != 0 && res[res.length - 1] != '..') {
					res.pop();
                } 
                else if (allowAboveRoot) {
					res.push('..');
				}
            } 
            else {
				res.push(p);
			}
		}

		return res;
    }
    
    // returns an array with empty elements removed from either end of the input
	// array or the original array if no elements need to be removed
	public static function trimArray(arr: Array<String>) {
		var lastIndex = arr.length - 1;
		var start = 0;
		// for (;start <= lastIndex;start++) {
		// 	if (arr[start])
		// 		break;
        // }
        while (start <= lastIndex) {
            final p = arr[start];
            if (p != null && p.length != 0)
                break;
            start++;
        }

		var end = lastIndex;
		// for (;end >= 0;end--) {
		// 	if (arr[end])
		// 		break;
        // }
        while (end >= 0) {
            if (arr[end] != null && arr[end].length != 0)
                break;
            end--;
        }

		if (start == 0 && end == lastIndex)
            return arr;
        
		if (start > end)
            return [];
        
		return arr.slice(start, end + 1);
    }

    // Regex to split a windows path into three parts: [*, device, slash,
    // tail] windows-only
    public static var splitDeviceRe = ~/^([a-zA-Z]:|[\\\/]{2}[^\\\/]+[\\\/]+[^\\\/]+)?([\\\/])?([\s\S]*?)$/;

    // Regex to split the tail part of the above into [*, dir, basename, ext]
    public static var splitTailRe = ~/^([\s\S]*?)((?:\.{1,2}|[^\\\/]+?|)(\.[^.\/\\]*|))(?:[\\\/]*)$/;

    public static function win32SplitPath(filename: String) {
        // Separate device+slash from tail
        var result = splitDeviceRe.match(filename);
        // var device = (result[1] || '') + (result[2] || ''),
        //     tail = result[3] || '';
        var device = splitDeviceRe.matched(1).nor('') + splitDeviceRe.matched(2).nor('');
        var tail = splitDeviceRe.matched(3).nor('');
        // Split the tail into dir, basename and extension
        var result2 = splitTailRe.exec(tail),
            dir = result2[1],
            basename = result2[2],
            ext = result2[3];
        return [device, dir, basename, ext];
    }

    public static function win32StatPath(path: String) {
        var result = splitDeviceRe.exec(path),
            device = result[1].nor(''),
            isUnc = !device.empty() && device.charAt(1) != ':';
        return {
            device: device,
            isUnc: isUnc,
            isAbsolute: isUnc || !result[2].empty(), // UNC paths are always absolute
            tail: result[3]
        };
    }

    public static function normalizeUNCRoot(device:String) {
        // return '\\\\' + device.replace(~/^[\\\/]+/, '').replace(~/[\\\/]+/g, '\\');
        var s = ~/^[\\\/]+/.replace(device, '');
        s = ~/[\\\/]+/g.replace(s, '\\');
        return '\\\\$s';
    }

    public static var isWindows:Bool = Sys.systemName()=='Windows';
}

class Posix {

}

class Win32 {
	public static function resolve(?fromPath:Array<String>, toPath:String) {
        var resolvedDevice = '', resolvedTail = '', resolvedAbsolute = false;
        var isUnc = false;

		var arguments = fromPath.nor([]);
		arguments.push(toPath);
		var i = arguments.length - 1;
		while (i >= -1) {
			var path:String;
			if (i >= 0) {
				path = arguments[i];
			} else if (resolvedDevice.empty()) {
				// path = process.cwd();
				path = Sys.getCwd();
			} else {
				// Windows has the concept of drive-specific current working
				// directories. If we've resolved a drive letter but not yet an
				// absolute path, get cwd for that drive. We're sure the device is not
				// an unc path at this points, because unc paths are always absolute.
				path = Sys.getEnv('=' + resolvedDevice);
				// Verify that a drive-local cwd was found and that it actually points
				// to our drive. If not, default to the drive's root.
				if (path.empty() || path.substr(0, 3).toLowerCase() != resolvedDevice.toLowerCase() + '\\') {
					path = resolvedDevice + '\\';
				}
			}

			// Skip empty and invalid entries
			if (!(path is String)) {
				throw new pm.Error('Arguments to path.resolve must be strings', 'TypeError');
			} else if (path.empty()) {
				i--;
				continue;
			}

			var result = Tools.win32StatPath(path),
				device = result.device,
				// isUnc = result.isUnc,
				isAbsolute = result.isAbsolute,
                tail = result.tail;
            isUnc = result.isUnc;

			if (!device.empty() && !resolvedDevice.empty() && device.toLowerCase() != resolvedDevice.toLowerCase()) {
				// This path points to another device so it is not applicable
				i--;
				continue;
			}

			if (resolvedDevice.empty()) {
				resolvedDevice = device;
			}

			if (!resolvedAbsolute) {
				resolvedTail = tail + '\\' + resolvedTail;
				resolvedAbsolute = isAbsolute;
			}

			if (resolvedDevice.b() && resolvedAbsolute) {
				break;
			}
		}

		// Convert slashes to backslashes when `resolvedDevice` points to an UNC
		// root. Also squash multiple slashes into a single one where appropriate.
		if (isUnc) {
			resolvedDevice = Tools.normalizeUNCRoot(resolvedDevice);
		}

		// At this point the path should be resolved to a full absolute path,
		// but handle relative paths to be safe (might happen when process.cwd()
		// fails)

		// Normalize the tail path
		resolvedTail = Tools.normalizeArray(~/[\\\/]+/.split(resolvedTail), !resolvedAbsolute).join('\\');

		return (resolvedDevice + (resolvedAbsolute ? '\\' : '') + resolvedTail).nor('.');
	}
}