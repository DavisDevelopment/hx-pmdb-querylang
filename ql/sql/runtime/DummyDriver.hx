package ql.sql.runtime;

import ql.sql.runtime.Sel.ISelDriver;
import pmdb.core.StructSchema;
import pmdb.core.Object;

import pm.Assert.aassert as assert;

class DummyDriver implements Sel.ISelDriver<Sel.DummyTable<Doc>, Doc, Dynamic> {
    public function new() {
        
    }
    
    public function connectSource(s: S) {
        assert(s != null && s.source != null && !s.source.name.empty());
    }

    public function iter(s:S, f:Doc->Bool) {
        s.source.data.foreach(f);
    }
}

private typedef S = Sel<Sel.DummyTable<Doc>, Doc, Dynamic>;