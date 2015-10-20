
class Hamming(object):
    def get_hamming_code(self,binary_str):
        binary_list = list(binary_str)
        for i in range(4):
            binary_list.insert(2**i-1,0)
            sum =0
        for j in [0,2,4,6,8,10]:
            if binary_list[j] == '1':
                sum +=1
        binary_list[0] = str(sum%2)
        sum = 0
        for j in [1,2,5,6,9,10]:
            if binary_list[j] == '1':
                sum += 1
        binary_list[1] = str(sum%2)
        sum = 0;
        for j in [3,4,5,6,11]:
            if binary_list[j] == '1':
                sum +=1
        binary_list[3] = str(sum%2)
        sum = 0;
        for j in [7,8,9,10,11]:
            if binary_list[j] == '1':
                sum +=1
        binary_list[7] = str(sum%2)
        b_str = ''.join(binary_list)
        return b_str

    def get_ascii(self,byte_str):
        binary_list = list(byte_str)
        for i in [7,3,1,0]:
            binary_list = binary_list[:i]+binary_list[i+1:]
        b_str = ''.join(binary_list)
        value = int(b_str,2)
        c = chr(value)
        return chr(value)

class HammingEncoder(Hamming):
    def encode(self,text):
        hamming = ''
        for c in text:
            binary_str = "{0:08b}".format(ord(c))
            hamming += self.get_hamming_code(binary_str)
        return hamming

class HammingDecoder(Hamming):
    def decode(self, text):
        """Return decoded text as a string."""
        out_string = ''
        while len(text) > 0:
            if text[0] == '\n':
                break
            byte_str = text[0:12]
            text = text[12:]
            out_string += self.get_ascii(byte_str)
        return out_string

class HammingChecker(Hamming):
    def check(self, text):
        pos =0
        positions = []
        while(len(text)>0):
            if text[0] == '\n':
                break
            byte_str = text[0:12]
            text = text[12:]
            c = self.get_ascii(byte_str)
            binary_str = "{0:08b}".format(ord(c))
            p = self.get_hamming_code(binary_str)
            for i in [0,1,3,7]:
                if p[i] != byte_str[i]:
                    positions += [pos]
        	    break
            pos += 1
        return positions

class HammingFixer(Hamming):
    def fix(self,text):
        fix_text = ''
        while(len(text)>0):
            if text[0] == '\n':
                break
            byte_str = text[0:12]
            text = text[12:]
            #temp = byte_str[0:12]
            c = self.get_ascii(byte_str)
            binary_str = "{0:08b}".format(ord(c))
            p = self.get_hamming_code(binary_str)
            pos = 0
            b =0
            for i in [0,1,3,7]:
                if p[i] != byte_str[i]:
                    b =1
                    pos += i+1
            if b==1:
        	byte_list = list(byte_str)
        	if byte_list[pos-1] == '0':
        	    byte_list[pos-1] = '1'
        	else:
        	    byte_list[pos-1] = '0'
        	t = ''.join(byte_list)
        	fix_text += t
            else:
                fix_text += byte_str
        return fix_text

class HammingError(Hamming):
    def createError(self,pos,text):
        byte_pos = pos/12
        bit_pos = pos%12
        target_str = text[byte_pos * 12:(byte_pos+1)*12]
        target_list = list(target_str)
        if target_list[bit_pos-1] =='1':
            target_list[bit_pos-1] ='0'
        else:
            target_list[bit_pos-1] ='1'
        error_str = ''.join(target_list)
        rst = text[:byte_pos * 12]+error_str+text[(byte_pos+1)*12:]
        return rst