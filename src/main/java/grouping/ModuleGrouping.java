package grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

/**
 * @Author: LaoGaoChuang
 * @Date : 2019/4/25 17:47
 */
public class ModuleGrouping implements CustomStreamGrouping, Serializable {
    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
       this.targetTasks = targetTasks;
        System.out.println("targetTasks:" + targetTasks);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<>();
        if (values.size() > 0) {
            String str = values.get(0).toString();
            if (str.isEmpty()) {
                boltIds.add(targetTasks.get(0));
            } else {
                boltIds.add(targetTasks.get(str.charAt(0) % targetTasks.size()));
            }
        }
        System.out.println("boltIds" + boltIds + ",taskId:" + taskId + ",values:" + values);
        return boltIds;
    }
}
