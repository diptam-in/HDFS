/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package client.interfaces;

/**
 *
 * @author diptam
 */
public interface DistributedFileWritable {
    public boolean put(String file, String path);
    public boolean makeDir(String dirPath);
}
