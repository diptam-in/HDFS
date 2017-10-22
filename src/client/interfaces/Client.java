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
public interface Client {
    public boolean put(String filename, String path);
    public byte[] get(String filename);
    public String list(String dirname);
    public boolean makeDirectory(String dirname);
}
