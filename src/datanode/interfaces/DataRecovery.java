/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datanode.interfaces;

import com.example.elements.BlockInfo.Block;

/**
 *
 * @author diptam
 */
public interface DataRecovery {
    public void recover(Block recoverBlock);
    public void remove(String blockNum);
}
