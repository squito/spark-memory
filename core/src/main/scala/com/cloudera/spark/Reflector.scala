// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.reflect.{Field, Method}

/* For example, I want to do this:
 *
 * sqlContext.catalog.client.getTable("default", "blah").properties
 *
 * but none of that is public to me in the shell.  Using this, I can now do:
 *
 * sqlContext.reflectField("catalog").reflectField("client").reflectMethod("getTable", Seq("default", "blah")).reflectField("properties")
 *
 * not perfect, but usable.
 */
object Reflector {

  def methods(obj: Any): Seq[String] = {
    _methods(obj.getClass()).map(_.getName()).sorted
  }

  def _methods(cls: Class[_]): Seq[Method] = {
    if (cls == null) Seq()
    else cls.getDeclaredMethods() ++ _methods(cls.getSuperclass())
  }

  def fields(obj: Any): Seq[String] = {
    _fields(obj.getClass()).map(_.getName()).sorted
  }

  def _fields(cls: Class[_]): Seq[Field] = {
    if (cls == null) Seq()
    else cls.getDeclaredFields() ++ _fields(cls.getSuperclass())
  }

  def findMethod(obj: Any, name: String): Method = {
    // TODO: handle scala's name munging, eg. org$apache$spark$sql$hive$HiveExternalCatalog$$makeQualified
    val method = _methods(obj.getClass()).find(_.getName() == name).get
    method.setAccessible(true)
    method
  }

  def get(obj: Any, name: String): Any = {
    val clz = obj.getClass()
    val fields = _fields(clz)
    fields.find(_.getName() == name).orElse {
      // didn't find an exact match, try again with name munging that happens for private vars
      fields.find(_.getName().endsWith("$$" + name))
    } match {
      case Some(f) =>
        f.setAccessible(true)
        f.get(obj)
      case None =>
        // not a field, maybe its actually a method in byte code
        val m = findMethod(obj, name)
        m.invoke(obj)
    }
  }

  def showFields(obj: Any): Unit = {
    fields(obj).foreach {
      println
    }
  }

  def showMethods(obj: Any): Unit = {
    methods(obj).foreach {
      println
    }
  }

  implicit class ReflectorConversions(obj: Any) {
    // TODO error msgs
    def reflectField(name: String): Any = {
      get(obj, name)
    }

    def reflectMethod(name: String, args: Seq[Object]): Any = {
      // TODO find a method that actually matches the args, not just the name, to deal w/ overloading
      findMethod(obj, name).invoke(obj, args: _*)
    }
  }

}