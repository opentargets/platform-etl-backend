

def file(name: String): Resource[IO, File] = Resource.make(openFile(name)))(file => close(file))