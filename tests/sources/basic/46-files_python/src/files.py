from pycompss.api.task import task
from pycompss.api.parameter import *


@task(fin = FILE, returns=str)
def fileIn(fin):
    print "TEST FILE IN"
    # Open the file and read the content
    fin_d = open(fin, 'r')
    content = fin_d.read()
    print "- In file content:\n", content
    # Close and return the content
    fin_d.close()
    return content
    
    
@task(finout = FILE_INOUT, returns=str)
def fileInOut(finout):
    print "TEST FILE INOUT"
    # Open the file and read the content
    finout_d = open(finout, 'r+')
    content = finout_d.read()
    print "- Inout file content:\n", content
    # Add some content
    content += "\n===> INOUT FILE ADDED CONTENT"
    finout_d.write("\n===> INOUT FILE ADDED CONTENT")
    print "- Inout file content after modification:\n", content
    # Close and return with the modification
    finout_d.close()
    return content
    
@task(fout = FILE_OUT, returns=str)
def fileOut(fout):
    print "TEST FILE OUT"
    # Open the file for writting and write some content
    fout_d = open(fout, 'w')
    content = "OUT FILE CONTENT"
    fout_d.write(content)
    print "- Out file content added:\n", content
    # Close and return the content written
    fout_d.close()
    return content


@task(returns=FILE)
def returnFile(filename, content):
    print "TEST RETURN FILE"
    # Open the file for writting and write some content
    fout_d = open(filename, 'w')
    fout_d.write(content)
    print "- Out file name: ", filename
    print "- Out file content added: ", content
    # Close and return the content written
    fout_d.close()
    return filename
  
@task(returns=(FILE, FILE))
def multireturnFile(content1, content2):
    print "TEST MULTIRETURN FILE"
    filename1 = 'retFile1'
    filename2 = 'retFile2'
    # Open the file for writting and write some content
    fout_d1 = open(filename1, 'w')
    fout_d1.write(content1)
    print "- Out file name: ", filename1
    print "- Out file content added: ", content1
    fout_d2 = open(filename2, 'w')
    fout_d2.write(content2)
    print "- Out file name: ", filename2
    print "- Out file content added: ", content2
    # Close and return the content written
    fout_d1.close()
    fout_d2.close()
    return filename1, filename2 
    
    
def main():
    from pycompss.api.api import compss_wait_on
    from pycompss.api.api import compss_open
    
    print "TEST PyCOMPSs WITH FILES"
    
    fin = 'infile'
    finout = 'inoutfile'
    fout = 'outfile'
    fretout = 'retFile'
    fretout1 = 'retFile1'
    fretout2 = 'retFile2'
    
    # Create initial files:
    fin_d = open(fin, 'w')
    finout_d = open(finout, 'w')
    fin_d.write('IN FILE CONTENT')
    finout_d.write('INOUT FILE INITIAL CONTENT')
    fin_d.close()
    finout_d.close()
    
    inRes = fileIn(fin)
    inoutRes = fileInOut(finout)
    outRes = fileOut(fout)
    retRes = returnFile(fretout, 'RETURN FILE CONTENTS')
    retResA, retResB = multireturnFile(fretout1, fretout2,"RETURN FILE CONTENTS A", "RETURN FILE CONTENTS B")  
    
    inRes = compss_wait_on(inRes)
    inoutRes = compss_wait_on(inoutRes)
    outRes = compss_wait_on(outRes)
    retRes = compss_wait_on(retRes)
    retResA = compss_wait_on(retResA)
    retResB = compss_wait_on(retResB)   

    fin_r = compss_open(fin)       # consolidated at the end of the execution. # Checked at test
    finout_r = compss_open(finout) # consolidated at the end of the execution. # Checked at test
    fout_r = compss_open(fout)     # consolidated at the end of the execution. # Checked at test
    foutRes_r = compss_open(fretout) # consolidated at the end of the execution. # Checked at test
    foutRes_r1 = compss_open(fretout1)     # consolidated at the end of the execution. # Checked at test
    foutRes_r2 = compss_open(fretout2)     # consolidated at the end of the execution. # Checked at test
    fin_r_c = fin_r.read()
    finout_r_c = finout_r.read()
    fout_r_c = fout_r.read()
    foutRes_r_c = foutRes_r.read()
    foutRes_r_c1 = foutRes_r1.read()
    foutRes_r_c2 = foutRes_r2.read()
    fin_r.close()
    finout_r.close()
    fout_r.close()
    foutRes_r.close()
    foutRes_r1.close()
    foutRes_r2.close()
    
  
    if inRes == 'IN FILE CONTENT': # and inRes == fin_r_c:
        print "- Test FILE_IN: OK"
    else:
        print "- Test FILE_IN: ERROR"
    
    if inoutRes == 'INOUT FILE INITIAL CONTENT\n===> INOUT FILE ADDED CONTENT': # and inoutRes == finout_r_c:
        print "- Test FILE_INOUT: OK"
    else:
        print "- Test FILE_INOUT: ERROR"
        
    if outRes == 'OUT FILE CONTENT': # and outRes == fout_r_c:
        print "- Test FILE_OUT: OK"
    else:
        print "- Test FILE_OUT: ERROR"
   
    # consolidated at the end of the execution. # Checked at test
    if rfoutRes_r_c == 'RETURN FILE CONTENTS':
        print "- Test RETURN FILE: OK"
    else:
        print "- Test RETURN FILE: ERROR"
    
    # consolidated at the end of the execution. # Checked at test
    if  foutRes_r_c1 == 'RETURN FILE CONTENTS A' and foutRes_r_c2 == 'RETURN FILE CONTENTS ':
        print "- Test MULTI RETURN with FILES: OK"
    else:
        print "- Test MULTI RETURN FILES: ERROR"
    
if __name__ == '__main__':
    main()
    
