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
    private ConcurrentSkipListMap<Integer, Container> mainMemory = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Integer, Container> secondaryMemory = new ConcurrentSkipListMap<>();
    private ReentrantReadWriteLock sstDumpLock = new ReentrantReadWriteLock();

    public void putData(int key, int value) {
        if (!sstDumpLock.readLock().tryLock()) {
            secondaryMemory.put(key, new Container(System.currentTimeMillis(), value));
        }
        else {
            try {
                //To protect multiple updates from other threads to map1
                mainMemory.put(key, new Container(System.currentTimeMillis(), value));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                sstDumpLock.readLock().unlock();
            }
        }
    }

    public void createSstFile() throws InterruptedException {
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
            return mainMemory.get(key).getValue();
        }
        if(secondaryMemory.containsKey(key)){
            return secondaryMemory.get(key).getValue();
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
            for (Map.Entry<Integer, Container> entry : mainMemory.entrySet()) {
                int keySize = Integer.toString(entry.getKey()).length();
                int valueSize = Integer.toString(entry.getValue().getValue()).length();
                int timestampSize = Long.toString(entry.getValue().getTimestamp()).length();
                String line = keySize + "," + entry.getKey() + "," + valueSize + "," + entry.getValue().getValue()
                        +","+timestampSize+"," + entry.getValue().getTimestamp()+ "\n";
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
        Thread compacter1 = new Thread(()->{
            try {
                tempLocks.createSstFile();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread compacter2 = new Thread(()->{
            try {
                tempLocks.createSstFile();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread normalThread3 = new Thread(() -> {
            for(int i =2001;i<3001;i++){
                tempLocks.putData(i, i+1);
            }
        });

        Thread normalThread4 = new Thread(() -> {
            for(int i =3001;i<4001;i++){
                tempLocks.putData(i, i+1);
            }
        });

        normalThread1.start();
        normalThread2.start();
        normalThread3.start();
        normalThread4.start();
        compacter1.start();
        compacter2.start();
        normalThread1.join();
        normalThread2.join();
        normalThread3.join();
        normalThread4.join();
        compacter2.join();
        compacter1.join();
        tempLocks.createSstFile();
    }
}
