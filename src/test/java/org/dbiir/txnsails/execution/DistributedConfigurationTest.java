package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.dbiir.txnsails.common.DistributedConfig;
import org.junit.jupiter.api.Test;

class DistributedConfigurationTest {
  @Test
  void readsParticipantIdsPortsIsolationAndPlacement() throws Exception {
    DistributedConfig configuration =
        new XmlMapper()
            .readValue(
                """
        <parameters>
          <instances>
            <instance><id>0</id><url>jdbc:postgresql://localhost/a</url><host>localhost</host><isolation>RC</isolation><rpcPort>23333</rpcPort></instance>
            <instance><id>1</id><url>jdbc:postgresql://localhost/b</url><host>localhost</host><isolation>SI</isolation><rpcPort>23334</rpcPort></instance>
          </instances>
          <tables><table><name>usertable</name><locations><location>0</location><location>1</location></locations></table></tables>
        </parameters>
        """,
                DistributedConfig.class);
    assertEquals(2, configuration.getInstances().getInstanceList().size());
    assertEquals("SI", configuration.getInstances().getInstanceList().get(1).getIsolation());
    assertEquals(23334, configuration.getInstances().getInstanceList().get(1).getRpcPort());
    assertEquals(
        java.util.List.of(0, 1),
        configuration.getTables().getTableList().get(0).getLocations().getLocationList());
  }
}
