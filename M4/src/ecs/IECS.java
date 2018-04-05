package ecs;

import java.util.Collection;
import java.util.Map;

public interface IECS {

    public boolean start(IECSNode server);

    public boolean stop(IECSNode server);

    public boolean shutdown();

    public IECSNode addNode(String cacheStrategy, int cacheSize);

    public boolean awaitNodes(int count, int timeout) throws Exception;

    public boolean removeNodes(Collection<IECSNode> nodeNames, boolean nodesCrashed);

    public Map<String, IECSNode> getNodes();

    public IECSNode getNodeByKey(String Key);
}
