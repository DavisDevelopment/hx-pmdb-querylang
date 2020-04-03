package ql.sql.common.ds;

// package pm;

import haxe.ds.Vector;

import pm.Assert.assert;
import pm.GrowthRate;

using pm.Arrays;
using pm.Vectors;

/**
 * 
 * A lightweight object pool
 * @see [polygonal.ds](https://github.com/polygonal/ds) for the implementation from which I derived this one
 */
@:generic
class ObjectPool<T> {
	/**
		The growth rate of the pool.
	**/
	public var growthRate:GrowthRate = GrowthRate.MILD;
	
	/**
		The current number of pooled objects.
	**/
	public var size(default, null):Int = 0;
	
	/**
		The maximum allowed number of pooled objects.
	**/
	public var maxSize(default, null):Int;
	
	private var mPool:  Array<T>;
   private var mFree : Array<T>;
	private var mCapacity:Int = 16;//how many pooled objects there can be
   
	private final mFactory: Void->T;
	private final mDispose: T->Void;
	
	public function new(factory:Void->T, ?dispose:T->Void, maxNumObjects:Int = -1) {
		maxSize = maxNumObjects;
		mFactory = factory;
      mDispose = dispose == null ? noop : dispose;
      
		mPool = Arrays.alloc( mCapacity );
		mFree = new Array();
   }

   static function noop(x: Dynamic):Void {}
   // inline function get_maxSize():Int return mCapacity;

	/*
	public function release(x: T) {
		assert(mPool.has(x), new pm.Error('Cannot release $x'));
		if (!mFree.has( x )) {
			mFree.push( x );
			mDispose( x );
		}
	}
	*/
	
	/**
		Fills the pool in advance with `numObjects` objects.
	**/
	public function preallocate(numObjects:Int) {
		assert(size == 0);
		
		size = mCapacity = numObjects;
		mPool.nullify();
		mPool = Arrays.alloc( size );
		for (i in 0...numObjects) {
		    mPool[i] = mFactory();
		}
	}
	
	/**
		Destroys this object by explicitly nullifying all objects for GC'ing used resources.
		Improves GC efficiency/performance (optional).
	**/
	public function free() {
		for (i in 0...mCapacity) 
		    mDispose(mPool[i]);
		Arrays.nullify( mPool );
		mPool = null;
		mFactory = null;
		mDispose = null;
	}
	
	/**
		Gets an object from the pool; the method either creates a new object if the pool is empty (no object has been returned yet) or returns an existing object from the pool.
		To minimize object allocation, return objects back to the pool as soon as their life cycle ends.
	**/
	public inline function get():T {
		return size > 0 ? mPool[--size] : mFactory();
	}
	
	/**
		Puts `obj` into the pool, incrementing `this.size`.
		
		Discards `obj` if the pool is full by passing it to the dispose function (`this.size` == `this.maxSize`).
	**/
	public inline function put(obj: T) {
		if (size == maxSize) {
			mDispose( obj );
      }
		else {
			if (size == mCapacity) {
			   resize();
         }
			mPool[size++] = obj;
		}
	}
	
	public function iterator():Iterator<T> {
		var i = 0;
		var s = size;
		var d = mPool;
		return {
			hasNext: () -> i < s,
			next: () -> d[i++]
		};
	}
	
	function resize() {
		var newCapacity = growthRate.compute( mCapacity );
		var t = Arrays.alloc( newCapacity );
		mCapacity = newCapacity;
		mPool.blit(0, t, 0, size);
		mPool = t;
	}
}
