package ql.sql.runtime;

import pm.map.ISet;
import haxe.ds.ReadOnlyArray;
import pm.map.AnySet;
import ql.sql.runtime.VirtualMachine.TableSpec;

class SelectSourceCollection {
   public final sources: ReadOnlyArray<TableSpec>;
   public final set: ISet<TableSpec>;
   
   public function new(a: Iterable<TableSpec>) {
      var manifest:ISet<TableSpec> = new AnySet((tbl: TableSpec) -> tbl.name);
      if ((a is pm.map.ISet<Dynamic>))
         manifest = cast a;
      for (tbl in a) {
         if (manifest.has(tbl)) {
            throw new pm.Error('Duplicate entry for "${tbl.name}"');
         }
         manifest.add(tbl);
      }
      
      this.set = manifest;
      this.sources = manifest.toArray();
   }

   public function iterator() return sources.iterator();

   public var length(get, never):Int;
   inline function get_length() return sources.length;
}