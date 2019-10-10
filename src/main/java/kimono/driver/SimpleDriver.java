package kimono.driver;

import kimono.client.KCDriverInfo;
import kimono.client.KCTenant;
import kimono.client.impl.AbstractDriver;
import kimono.client.impl.tasks.TaskAck;
import kimono.client.tasks.KCTask;
import kimono.client.tasks.KCTaskAck;
import kimono.client.tasks.KCTaskPoller;
import kimono.client.tasks.KCTaskType;

public class SimpleDriver extends AbstractDriver {

	@Override
	protected KCDriverInfo newDriverInfo() {
		return new KCDriverInfo("SimpleDriver");
	}

	@Override
	protected void configureTaskHandlers(KCTaskPoller poller) {

		// Handle Sync Start/Sync End events
		poller.setTaskHandler(KCTaskType.SYNC_EVENT, this::handleSyncEvent);

		// Handle Data Events
		poller.setTaskHandler(KCTaskType.DATA_EVENT, (tenant,task)->{ 
			System.out.println(task); 
			return TaskAck.success(); 
		} );
		
		// An example of how to use a Predicate to further filter Tenants based on 
		// local app/driver state that is not known to Kimono. For example, you may 
		// have determined a tenant is off-line in the application and therefore don't 
		// want to process tasks for it. This can be removed if there is no app
		// state that needs to be checked when iterating tenants.
		poller.setPredicate(this::isProcessTenant);
	}

	/**
	 * Determine whether or not tasks should be processed for a given tenant.
	 * Called for each tenant when iterating tenants in the Task Loop.
	 * @param tenant
	 * @return
	 */
	protected boolean isProcessTenant( KCTenant tenant ) {
		return true;
	}

	/**
	 * Handle a Sync Event.
	 * 
	 * @param tenant The tenant
	 * @param task The task
	 * 
	 * @return A KCTaskAck to acknowledge or retry the task
	 */
	protected KCTaskAck handleSyncEvent(KCTenant tenant, KCTask task) {
		System.out.println(task);
		return TaskAck.success();
	}

	/**
	 * Handle a Data Event.
	 * 
	 * @param tenant The tenant
	 * @param task The task
	 * 
	 * @return A KCTaskAck to acknowledge or retry the task
	 */
	protected KCTaskAck handleDataEvent(KCTenant tenant, KCTask task) {
		System.out.println(task);
		return TaskAck.success();
	}

	public static void main(String[] args) {
		try {
			new SimpleDriver().parseCommandLine(args).run();
		} catch (Exception ex) {
			System.err.println(ex);
		}
	}
}
