package mammuthus.yarn.util

/**
 * 8/31/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ParentClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

  override def findClass(name: String) = {
    super.findClass(name)
  }

  override def loadClass(name: String): Class[_] = {
    super.loadClass(name)
  }

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    super.loadClass(name, resolve)
  }

}