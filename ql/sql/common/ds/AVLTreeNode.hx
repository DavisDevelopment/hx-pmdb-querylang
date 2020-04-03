package ql.sql.common.ds;

import pm.ObjectPool;

class AVLTreeNode<Key, Item> {
   private function new() {
      
   }

   static var pool:ObjectPool<AVLTreeNode<Dynamic, Dynamic>>;

   static function __init__() {
      pool = new ObjectPool(
         AVLTreeNode.ctor,
         AVLTreeNode.dtor,
      )
   }
}