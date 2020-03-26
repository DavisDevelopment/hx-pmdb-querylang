package ql.sql.runtime.plan;

import haxe.iterators.*;
import pm.iterators.*;

import pm.ObjectPool;

using pm.Arrays;
using Lambda;
using pm.Iterators;

class Itr<T> {
   public function hasNext():Bool return false;
   public function next():T throw new pm.Error.NotImplementedError();
   public function prev():T throw new pm.Error.NotImplementedError();
   public function reset():Void throw new pm.Error.NotImplementedError();
   public function dispose():Void return ;
}

class ArrayItr<T> extends Itr<T> {
   var a:Array<T>;
   var idx: Int = 0;
   var run: Int = 1;

   public function new(array, index=0, step=1) {
      this.a = array;
      this.idx = index;
      this.run = step;
   }
   override function next():T {
      var i = idx;
      idx += run;
      return a[i];
   }
   
   override function prev():T {
      var i = idx;
      idx -= run;
      return a[i];
   }
   
   override function dispose() {
      a = null;
      idx = -1;
      run = 0;
   }
   
   override function reset() {
      // super.reset();
   }

   override function hasNext():Bool {
      // return super.hasNext();
      if (run > 0)
         return idx + run < a.length;
      else if (run < 0)
         return idx + run >= 0;
      else
         throw new pm.Error('run cannot be 0');
   }
}

#if xxwph
typedef ItrStep<T> = {
   @:optional var value: T;
   @:optional var error: Dynamic;
   var done: Bool;
};

@:nullSafety(Strict)
class Itr<T> {
   public var current:Null<Step<T>> = null;
   public var isStarted(default, null):Bool = false;
   public var isEnded(default, null):Bool = false;

   public function new(api) {
      // this.api = api;
   }

   public function begin() {
      if (isStarted) throw new pm.Error('Already started');

      isStarted = true;
      // api.begin();
   }

   public function end() {
      if (isEnded) throw new pm.Error('Already ended');
      isEnded = true;
      // api.end();
   }

   public function next():Step<T> {
      assert(isStarted && !isEnded, new pm.Error('No, sha'));
      // throw new pm.Error.NotImplementedError();
      current = step();
      if (current.done) {
         if (!isEnded)
            end();
      }

      return current;
   }

   function step():Step<T> {
      return cast TailStep.instance;
   }

   private function hx_hasNext():Bool {
      return current == null || !current.done;
   }

   private function hx_next():T {
      if (!isStarted) begin();
      var step = next();
      if (step.error != null)
         throw step.error;
      var ret:T = step.value;
      return ret;
   }
}

private class Step<T> {
   public var done: Bool;
   public var value: T;
   public var error: Null<Dynamic> = null;
   public function new(done) {
      this.done = done;
   }
}
private class YieldStep<T> extends Step<T> {
   public function new(value, done=false) {
      super(done);
      this.value = value;
   }
}
private class TailStep extends Step<Dynamic> {
   public static var instance:TailStep = new TailStep(true);
}
private class ThrowStep<T> extends Step<T> {
   // public var error:Dynamic;
   public function new(error) {
      super(true);
      this.error = error;
   }
}
#end