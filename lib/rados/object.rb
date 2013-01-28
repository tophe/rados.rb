# -*- coding: utf-8 -*-
module Rados
  class RObject

    attr_accessor :ioctx, :oid

    @@size_xattr = "Content-Length"
    
    
    def initialize(oid, ioctx)
      @ioctx = ioctx
      @oid = oid
      @offset = 0
    end

    
    # set x_attribut
    def set_attribut(key,value)
      #rados_setxattr
    end
    
    # get x_attribut
    def get_attribut(key)
      #rados_getxattr
    end
    
    # get or itter through x_attributs
    def attributs()
      if block_given?
        rados_getxattrs.each { |key,value| 
          yield key,value
        }
      else
        return rados_getxattrs
      end
      
    end
    
    # Reads length bytes from the I/O stream.
    # length must be a non-negative integer or nil.
    # If length is a positive integer, it try to read length bytes
    # without any conversion (binary mode).
    #
    # It returns nil or a string whose length is 1 to length
    # bytes. nil means it met EOF at beginning. The 1 to length-1
    # bytes string means it met EOF after reading the result. The
    # length bytes string means it doesn’t meet EOF. The resulted
    # string is always ASCII-8BIT encoding.
    #
    # If length is omitted or is nil, it reads until EOF and the
    # encoding conversion is applied. It returns a string even if EOF
    # is met at beginning.
    #
    # If length is zero, it returns "".
    #
    # If the optional buffer argument is present, it must reference a
    # String, which will receive the data.
    #
    # At end of file, it returns nil or "" depend on
    # length. ios.read() and ios.read(nil) returns
    # "". ios.read(positive-integer) returns nil.
    def read()
      attr_size = get_attribut(@@size_xattr)
      unless attr_size.nil?
        length = attr_size unless attr_size.nil?
        data = @ioctx.read(@oid, length, @offset)
        @offset += data.size
      else
        data = ""
        #read til the IO end
        while(true)
          buff_size = 16 * 1024
          get_d = @ioctx.read(@oid, buff_size, @offset)
          unless  get_d.empty?
            data += get_d
            @offset += get_d.size
          else
            break
          end
          
        end
        
      end
      Marshal.load(data)
      
    end

    
    # Writes the given string to the object. The stream must be opened
    # for writing. If the argument is not a string, it will be
    # converted to a string using to_s. Returns the number of bytes
    # written.
    def write(s)
      binary_to_store = Marshal.dump(s)
      bytes = @ioctx.write(@oid, binary_to_store, binary_to_store.size, @offset)
      # add an xattr :
      set_attribut(@@size_xattr, binary_to_store.size)
      
      @offset += bytes
      bytes
    end

    # Positions offset to the beginning of input
    def rewind
      @offset = 0
    end

    # Returns the current offset (in bytes)
    def tell
      @offset
    end
    alias :pos :tell

    # Seeks to a given offset in the object according to the value of
    # whence (see IO#seek for values of whence). Returns the new
    # offset.
    def seek(amount, whence = IO::SEEK_SET)
      case whence
      when IO::SEEK_CUR
        @offset += amount
      when IO::SEEK_SET
        raise Errno::EINVAL if amount < 0
        @offset = amount
      when IO::SEEK_END
        @offset = size + amount
      end
      @offset
    end
  end
end
