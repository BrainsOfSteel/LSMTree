package org.example;

import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

@Component
public class MergeSSTTables {
    public void mergeSSTTable(){
        File directoryName = new File(Util.SST_TABLE_DIRECTORY);
        File[] fileList = directoryName.listFiles();

        if(fileList == null || fileList.length == 0){
            return;
        }

        List<File> fileToDelete = new ArrayList<>();
        List<File> actualFileList = new ArrayList<>();
        for(File file : fileList){
            if(file.getName().contains("temp")){
                fileToDelete.add(file);
            }
            else{
                actualFileList.add(file);
            }
        }

        if(fileToDelete.size() > 0){
            deleteFiles(fileToDelete);
        }

        if(actualFileList.size() == 0 || actualFileList.size() == 1){
            return;
        }
        String mergeFileName = null;
        for(int i =0;i<actualFileList.size();){
            if(mergeFileName == null){
                mergeFileName = mergeFile(actualFileList.get(i).getAbsolutePath(), actualFileList.get(i+1));
                actualFileList.get(i).delete();
                actualFileList.get(i+1).delete();
                i=i+2;
            }
            else {
                String originalFileName = mergeFileName;
                mergeFileName = mergeFile(mergeFileName, actualFileList.get(i));
                actualFileList.get(i).delete();
                File originalFile = new File(originalFileName);
                originalFile.delete();
                i++;
            }
        }
    }

    private String mergeFile(String mergeFileName, File file) {
        String tempFileName = Util.SST_TABLE_DIRECTORY+"/"+ Util.SST_TABLE_PREFIX + System.currentTimeMillis();
        try(BufferedReader fileReader1 = new BufferedReader(new FileReader(mergeFileName));
            BufferedReader fileReader2 = new BufferedReader(new FileReader(file));
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFileName))){
            String line1 = fileReader1.readLine();
            String line2 = fileReader2.readLine();
            CRC32 crc32 = new CRC32();
            while(line1 != null && line2 != null){
                String[] lineContent1 = line1.split("\\|");
                Util.crcChecks(lineContent1[0], lineContent1[1], crc32);
                crc32.reset();
                String[] lineContent2 = line2.split("\\|");
                Util.crcChecks(lineContent2[0], lineContent2[1], crc32);
                crc32.reset();
                int cmp = compare(lineContent1[1], lineContent2[1]);
                if(cmp < 0){
                    fileWriter.write(line1+"\n");
                    fileWriter.flush();
                    line1 = fileReader1.readLine();
                }
                else if(cmp == 0){
                    int cmpTs = compareTs(lineContent1[1], lineContent2[1]);
                    if(cmpTs <= 0){
                        fileWriter.write(line2+"\n");
                        fileWriter.flush();
                    }
                    else {
                        fileWriter.write(line1 +"\n");
                        fileWriter.flush();
                    }
                    line2 = fileReader2.readLine();
                    line1 = fileReader1.readLine();
                }
                else{
                    fileWriter.write(line2 +"\n");
                    fileWriter.flush();
                    line2 = fileReader2.readLine();
                }
            }
            while(line1 != null){
                String[] lineContent1 = line1.split("\\|");
                Util.crcChecks(lineContent1[0], lineContent1[1], crc32);
                crc32.reset();
                fileWriter.write(line1 +"\n");
                fileWriter.flush();
                line1 = fileReader1.readLine();
            }

            while(line2 != null){
                String[] lineContent2 = line2.split("\\|");
                Util.crcChecks(lineContent2[0], lineContent2[1], crc32);
                crc32.reset();
                fileWriter.write(line2 +"\n");
                fileWriter.flush();
                line2 = fileReader2.readLine();
            }
        }catch(AssertionError e){
            e.printStackTrace();
            System.out.println("Corrupted file....");
            System.exit(1);
        }
        catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
        return tempFileName;
    }

    private int compareTs(String line1, String line2){
        String [] content1 = line1.split(",");
        String [] content2 = line2.split(",");

        Long ts1 = Long.parseLong(content1[5]);
        Long ts2 = Long.parseLong(content2[5]);
        return ts1.compareTo(ts2);
    }

    private int compare(String line1, String line2) {
        String [] content1 = line1.split(",");
        String [] content2 = line2.split(",");

        Integer key1 = Integer.parseInt(content1[1]);
        Integer key2 = Integer.parseInt(content2[1]);
        if(key1.compareTo(key2) < 0){
            return -1;
        }
        else if(key1.compareTo(key2) == 0){
            return 0;
        }
        else{
            return 1;
        }
    }

    private void deleteFiles(List<File> fileToDelete) {
        for(File file : fileToDelete){
            file.delete();
        }
    }

    public static void main(String[] args) {
        MergeSSTTables mergeSSTTables = new MergeSSTTables();
        mergeSSTTables.mergeSSTTable();
//        Util.runSanity("/Volumes/Seagate/PersonalProject2/zookeeper-main/recipe/locks/SstDirectory/SST_1689096869911");
    }
}
