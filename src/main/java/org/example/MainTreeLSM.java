package org.example;

import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

@Component
public class MainTreeLSM {
    private ConcurrentSkipListMap<Integer, Integer> mainMemory = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Integer, Integer> secondaryMemory = new ConcurrentSkipListMap<>();
    private ReentrantReadWriteLock sstDumpLock = new ReentrantReadWriteLock();

    public void putData(int key, int value) {
        if (!sstDumpLock.readLock().tryLock()) {
            secondaryMemory.put(key, value);
        }
        else {
            try {
                //To protect multiple updates from other threads to map1
                mainMemory.put(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                sstDumpLock.readLock().unlock();
            }
        }
    }

    public void compaction() throws InterruptedException {
        try{
            sstDumpLock.writeLock().lock();
            dumpToFile();
            mainMemory.clear();
            synchronized(secondaryMemory){
                mainMemory.clear();
                mainMemory.putAll(secondaryMemory);
                secondaryMemory.clear();
            }
        }finally {
            sstDumpLock.writeLock().unlock();
        }
    }

    public Integer searchKey(int key){
        if(mainMemory.containsKey(key)){
            return mainMemory.get(key);
        }
        if(secondaryMemory.containsKey(key)){
            return secondaryMemory.get(key);
        }

        return searchKeyInFiles(key);
    }

    private Integer searchKeyInFiles(int key) {
        File directory = new File(Util.SST_TABLE_DIRECTORY);
        File[] files = directory.listFiles();
        if(files == null || files.length == 0){
            return null;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for(File file: files){
            Integer value = searchKeyInFiles(file, key);
            if(value != null){
                return value;
            }
        }

        return null;
    }

    private Integer searchKeyInFiles(File file, int key){
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file))){
            String line = null;
            CRC32 crc32 = new CRC32();
            while((line = bufferedReader.readLine()) !=null){
                String[] lineData = line.split("\\|");
                long crc = Long.parseLong(lineData[0]);
                crc32.update((lineData[1]+"\n").getBytes());
                long crcFromFile = crc32.getValue();
                crc32.reset();
                if(crcFromFile != crc){
                    throw new RuntimeException("Corrupted data");
                }
                String[] content = lineData[1].split(",");
                if(Integer.parseInt(content[1]) == key){
                    return Integer.parseInt(content[3]);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    private void dumpToFile(){
        String fileName = Util.SST_TABLE_DIRECTORY+"/" + Util.SST_TABLE_PREFIX+System.currentTimeMillis();
        CRC32 crc32 = new CRC32();
        try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName))) {
            for (Map.Entry<Integer, Integer> entry : mainMemory.entrySet()) {
                int keySize = Integer.toString(entry.getKey()).length();
                int valueSize = Integer.toString(entry.getValue()).length();
                String line = keySize + "," + entry.getKey() + "," + valueSize + "," + entry.getValue()+"\n";
                crc32.update(line.getBytes());
                long crc = crc32.getValue();
                crc32.reset();
                fileWriter.write(crc+"|" + line);
                fileWriter.flush();
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Catastrophic failure....Shutting down");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MainTreeLSM tempLocks = new MainTreeLSM();
        Thread normalThread1 = new Thread(() -> {
            for(int i =0;i<1000;i++){
                tempLocks.putData(i, i+1);
            }
        });
        Thread normalThread2 = new Thread(() -> {
            for(int i =1000;i<2001;i++){
                tempLocks.putData(i, i+1);
            }
        });
        Thread compacter = new Thread(()->{
            try {
                tempLocks.compaction();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        normalThread1.start();
        normalThread2.start();
        compacter.start();
        normalThread1.join();
        normalThread2.join();
        compacter.join();
        tempLocks.compaction();
    }
}
