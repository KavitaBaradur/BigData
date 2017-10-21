package task;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class DataGeneration {
    public static void main(String[] args) throws IOException {
        points(8000000);
        rectangles(200000);
    }

    static void points(int num) throws IOException {
        Random random = new Random();
        FileOutputStream file = new FileOutputStream(new File("points.csv"));
        BufferedOutputStream buffer = new BufferedOutputStream(file);
        int x_ray, y_ray = 0;
        String store = null;
        for (int i = 0; i < num; i++) {
            x_ray = random.nextInt(10000);
            y_ray = random.nextInt(10000);
            store = x_ray + "," + y_ray;
            if (i < num) {
                store = store + "\n";
            }
            buffer.write(store.getBytes());
            buffer.flush();
        }
        buffer.close();
    }


    static void rectangles(int num) throws IOException {
        Random random = new Random();
        FileOutputStream file = new FileOutputStream(new File("retangles.csv"));
        BufferedOutputStream buffer = new BufferedOutputStream(file);
        int x_ray, y_ray, x_right, y_right, width, height = 0;
        String store = null;
        for (int i = 0; i < num; i++) {
            x_ray = random.nextInt(10000);
            y_ray = random.nextInt(10000);
            width = random.nextInt(5);
            height = random.nextInt(20);
            x_right = x_ray + width;
            y_right = y_ray + height;
            store = "r" + i + "," + x_ray + "," + y_ray + "," + x_right + "," + y_right;
            if (i < num) {
                store = store + "\n";
            }
            buffer.write(store.getBytes());
            buffer.flush();
        }
        buffer.close();
    }
}


