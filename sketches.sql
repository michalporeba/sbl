:on error exit
:setvar DomainName "//SBL"
:setvar SchemaName "sbl"
:setvar ConversationTimeout 3600
:setvar DB "QA02152016"
:setvar DB "Communications"

use [$(DB)]
go
alter database [$(DB)] set enable_broker with rollback immediate
alter database [$(DB)] set trustworthy on with rollback immediate 
go
alter authorization on database::[$(DB)] to sa

exec sp_addmessage  @msgnum = 73001, @severity = 10, @with_log = 'true', @msgtext = 'Failed to process a message in queue %s.%s. Scheduled reprocessing. Source: %s in line %i. Error: (%i) %s.', @replace='replace'
exec sp_addmessage  @msgnum = 73002, @severity = 10, @with_log = 'true', @msgtext = 'Failed to process a message in queue %s.%s. This was the final attempt. Source: %s in line %i. Error: (%i) %s.', @replace='replace'

if not exists(select 1 from sys.schemas where name = '$(SchemaName)')
exec('create schema $(SchemaName)')
go

if not exists(select 1 from sys.tables where name = 'Conversations' and schema_id=schema_id('$(SchemaName)'))
/* table to store open, reusable conversations per SPID */
create table $(SchemaName).Conversations (
	 FromService		sysname not null
	,ToService			sysname not null
	,OnContract			sysname not null
	,Spid				int not null
	,Handle				uniqueidentifier not null
	,StartedOn			datetimeoffset not null
	,constraint PK_$(SchemaName)_Conversations primary key (FromService, ToService, OnContract, Spid)
	,constraint UQ_$(SchemaName)_Conversation_Handle unique (Handle)
);
go

if not exists(select 1 from sys.tables where name = 'Topics' and schema_id=schema_id('$(SchemaName)'))
/* table to store open, reusable conversations per topic*/
create table $(SchemaName).Topics (
	 FromService		sysname not null
	,ToService			sysname not null
	,OnContract			sysname not null
	,Topic				uniqueidentifier not null
	,Handle				uniqueidentifier not null
	,StartedOn			datetimeoffset not null
	,constraint PK_$(SchemaName)_Topics primary key (FromService, ToService, OnContract, Topic)
	,constraint UQ_$(SchemaName)_Topic_Handle unique (Handle)
);
go

if not exists(select 1 from sys.tables where name = 'Messages' and schema_id=schema_id('$(SchemaName)'))
/* table to store messages with errors to retry later */
create table $(SchemaName).[Messages] (
	 Id					smallint not null identity 
	,ToService			sysname not null
	,OnContract			sysname not null
	,MessageType		sysname not null
	,MessageRetention	smallint not null
	,RecordRetention	smallint not null
	,MaxAttempts		smallint not null
	,constraint PK_$(SchemaName)_Messages primary key (ToService, OnContract, MessageType)
);
go

if not exists(select 1 from sys.tables where name = 'InternalErrors' and schema_id=schema_id('$(SchemaName)'))
/* table to store messages with errors to retry later */
create table $(SchemaName).InternalErrors (
	 Id					int not null identity
	,Ts					datetimeoffset not null
	,[Action]			sysname not null
	,[ErrorProcedure]	sysname null
	,[ErrorLine]		int null
	,[ErrorNumber]		int null
	,[ErrorMessage]		nvarchar(max) null
	,[Context]			xml null
	,constraint PK_$(SchemaName)_InternalErrors primary key (Id)	 
);
go

if not exists(select 1 from sys.tables where name = 'ProcessingErrors' and schema_id=schema_id('$(SchemaName)'))
/* table to store messages with errors to retry later */
create table $(SchemaName).ProcessingErrors (
	 Id					int not null identity
	,[Service]			sysname not null
	,[Contract]			sysname not null
	,[MessageType]		sysname not null
	,[MessageBody]		xml null
	,[ErrorProcedure]	sysname null
	,[ErrorLine]		int null
	,[ErrorNumber]		int null
	,[ErrorMessage]		nvarchar(max) null
	,Attempts			smallint not null
	,FirstAttemptOn		datetimeoffset not null
	,LastAttemptOn		datetimeoffset not null
	,constraint PK_$(SchemaName)_ProcessingErrors primary key (Id)	 
);
go


if not exists(select 1 from sys.tables where name = 'ProcessingLog' and schema_id=schema_id('$(SchemaName)'))
/* table to store messages with errors to retry later */
create table $(SchemaName).ProcessingLog (
	 Id						bigint not null identity 
	,MessageId				smallint not null
	,Attempt				smallint not null
	,Successful				bit not null
	,RequestedProcessingOn	datetimeoffset null
	,StartedProcessingOn	datetimeoffset not null
	,FinishedProcessingOn	datetimeoffset not null
	,RequestingTime			as datediff(second, RequestedProcessingOn, StartedProcessingOn) persisted
	,ProcessingTime			as datediff(ms, StartedProcessingOn, FinishedProcessingOn) persisted
	,MessageBody			xml null
	,constraint PK_$(SchemaName)_ProcessingLog primary key (Id)
);

if not exists(select 1 from sys.columns where name = 'RequestedProcessingOn' and object_id = object_id('$(SchemaName).ProcessingLog'))
alter table $(SchemaName).ProcessingLog add RequestedProcessingOn datetimeoffset null
go
if not exists(select 1 from sys.columns where name = 'RequestingTime' and object_id = object_id('$(SchemaName).ProcessingLog'))
alter table $(SchemaName).ProcessingLog add RequestingTime as datediff(second, RequestedProcessingOn, StartedProcessingOn) persisted
go

if not exists(select 1 from sys.views where name = 'ReusableConversations' and schema_id=schema_id('$(SchemaName)'))
exec('create view $(SchemaName).ReusableConversations as select 1 tmp')
go
alter view $(SchemaName).ReusableConversations 
as
select Handle, FromService, ToService, OnContract, 'SPID: ' + convert(varchar(max), Spid) Topic, StartedOn
	,ce.[state]			[State]
	,ce.[state_desc]	StateDescription
	,ce.dialog_timer	DialogTimer
	,ce.send_sequence	SendMessages
	,ce.[priority]		[Priority]
from sb.Conversations c
left outer join sys.conversation_endpoints ce on c.Handle = ce.conversation_handle
union all 
select Handle, FromService, ToService, OnContract, 'Topic: ' + convert(varchar(max), Topic) Topic, StartedOn
	,ce.[state]			[State]
	,ce.[state_desc]	StateDescription
	,ce.dialog_timer	DialogTimer
	,ce.send_sequence	SendMessages
	,ce.[priority]		[Priority]
from sb.Topics c
left outer join sys.conversation_endpoints ce on c.Handle = ce.conversation_handle
go


go
if not exists(select 1 from sys.objects where name = 'GetLocalServiceName' and schema_id=schema_id('$(SchemaName)') and [type] = 'FN')
exec('create function $(SchemaName).GetLocalServiceName() 
returns sysname
as begin
	return (''$(DomainName)/DB/''+db_name())
end
')
go

go
if not exists(select 1 from sys.objects where name = 'GetDefaultTopicId' and schema_id=schema_id('$(SchemaName)') and [type] = 'FN')
exec('create function $(SchemaName).GetDefaultTopicId() 
returns uniqueidentifier
as begin
	return (convert(uniqueidentifier, ''00000000-0000-0000-0000-000000000000''))
end
')
go


if not exists(select 1 from sys.views where name = 'HealthCheck' and schema_id=schema_id('$(SchemaName)'))
exec('create view $(SchemaName).HealthCheck as select 1 tmp')
go
alter view $(SchemaName).HealthCheck
as
select
	 case when [Broker] = 0 then 'Disabled Broker'
		when isnull(Activator,'')='' then 'No Activator'
		when [Activation] = 0 then 'Disabled Activation'
		when [CanReceive] = 0 then 'Disabled Receiving'
		when [CanEnqueue] = 0 then 'Disabled Enqueuing'
		when ActiveReaders > 0 then 'Processing'
		when ActiveConversations > 0 then 'Conversing'
		else ''
	 end [Status]
	,* 
from (
	select 
		 case when s.service_id <= 3 then 'SYSTEM'
			when s.name like sb.GetLocalServiceName() then 'LOCAL'
			when s.name like '$(DomainName)/Internal/%' then 'INTERNAL'
			else 'EXTERNAL'
		 end	[Type]
		,s.name																[Service]
		,'['+schema_name(q.schema_id)+'].['+ q.name + ']'					[Queue]
		,(	select sum(p.rows) 
			from sys.objects o
			inner join sys.partitions p on o.object_id = p.object_id
			where o.parent_object_id = q.object_id and index_id = 1
		 )																	[Messages]
		,q.activation_procedure												[Activator]
		,(	select count(*) 
			from sys.dm_broker_activated_tasks
			where database_id=db_id() and queue_id = q.object_id
		 )																	[ActiveReaders]
		,q.max_readers														[MaxReaders]
		,(	select is_broker_enabled 
			from sys.databases
			where database_id = db_id()
		 )																	[Broker]
		,q.is_activation_enabled											[Activation]
		,q.is_receive_enabled												[CanReceive]
		,q.is_enqueue_enabled												[CanEnqueue] 
		,(	select count(distinct conversation_id) 
			from sys.conversation_endpoints ce
			where s.service_id = ce.service_id
				and [state] = 'CO' --only conversing
		 )																	[ActiveConversations]
		,isnull((	select name 'Contract' 
			from sys.service_contracts sc 
			inner join sys.service_contract_usages scu on scu.service_contract_id = sc.service_contract_id
			where scu.service_id = s.service_id
			for xml path(''), root('Contracts'), type
		 ),'(Initiator Only)')												[SupportedContracts]
	from sys.services s
	inner join sys.service_queues q on s.service_queue_id = q.object_id
) t
go

/* programability */
if not exists(select 1 from sys.procedures where name = 'GetMessageConfiguration' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).GetMessageConfiguration as begin print 1 end')
go
alter procedure $(SchemaName).GetMessageConfiguration 
	 @service			sysname
	,@contract			sysname
	,@messageType		sysname 
	,@messageId			smallint output
	,@messageRetention	bit output 
	,@recordRetention	bit output 
	,@maxAttempts		smallint output
as
begin
	set xact_abort off;
	set nocount on 

	select 
		 @messageId = Id
		,@messageRetention = case when MessageRetention>=0 then 1 else 0 end
		,@recordRetention = case when RecordRetention>=0 then 1 else 0 end 
		,@maxAttempts = MaxAttempts
	from $(SchemaName).[Messages] (nolock)
	where ToService = @service and OnContract = @contract and MessageType = @messageType

	if @@rowcount = 0 
	begin
		insert into $(SchemaName).[Messages] (ToService, OnContract, MessageType, MessageRetention, RecordRetention, MaxAttempts)
		values (@service, @contract, @messageType, -1, 7, 5)

		select 
			 @messageId = scope_identity()
			,@messageRetention = -1		--by default do not retain message
			,@recordRetention = 7		--by default keep message log for 7 days 
			,@maxAttempts = 3			--by default try up to 3 times. 2 reattempts. 
	end
end
go

if not exists(select 1 from sys.procedures where name = 'GetConversation' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).GetConversation as begin print 1 end')
go
alter procedure $(SchemaName).GetConversation 
	 @from			sysname
	,@to			sysname
	,@contract		sysname
	,@handle		uniqueidentifier output
as begin
	set xact_abort off;
	set nocount on;
	declare @conversations table (Handle uniqueidentifier not null)

	begin transaction 

	update top(1) $(SchemaName).Conversations with (readpast)
	set Spid = @@SPID 
	output inserted.Handle into @conversations(Handle)
	where FromService = @from
		and ToService = @to
		and OnContract = @contract
		and (Spid = -1 or Spid = @@spid)

	if @@rowcount > 0
	begin
		--there is an existing dialog. use it. 
		select @handle = Handle from @conversations
	end
	else
	begin
		--there is no available conversation. create one. 
		begin dialog conversation @handle
		from service @from
		to service @to
		on contract @contract
		with encryption = off 

		insert into $(SchemaName).Conversations (FromService, ToService, OnContract, Handle, Spid, StartedOn)
		values (@from, @to, @contract, @handle, @@spid, sysdatetimeoffset())
	end

	begin try 
		begin conversation timer (@handle) timeout = $(ConversationTimeout) --timeout in seconds. conversation will stay 
	end try 
	begin catch 
		--rollback transaction 
		--begin transaction
		--most likely the conversation was not in the usable state. it will be retried, but log the internal error 
		insert into $(SchemaName).InternalErrors (Ts, [Action], [ErrorProcedure], [ErrorLine], [ErrorNumber], [ErrorMessage], [Context])
		select sysdatetimeoffset(), 'Setting conversation timer', error_procedure(), error_line(), error_number(), error_message()
			,(select @from '@FromService', @to '@ToService', @contract '@OnContract' for xml path('Context')) --context
	end catch

	commit 
end
go

if not exists(select 1 from sys.procedures where name = 'ReleaseConversation' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReleaseConversation as begin print 1 end')
go
alter procedure $(SchemaName).ReleaseConversation
	 @from			sysname
	,@to			sysname
	,@contract		sysname
	,@handle		uniqueidentifier
as begin
	set xact_abort off;
	set nocount on;
	begin tran

	update $(SchemaName).Conversations set Spid = -1
	where FromService = @from
		and ToService = @to
		and OnContract = @contract
		and Handle = @handle 

		if @@rowcount != 1
		begin
			declare @msg varchar(max) = convert(varchar(max), @handle)
			raiserror('$(SchemaName).ReleaseConversation: Failed to release dialog handle %s from the $(SchemaName).Conversations', 16, 1, @msg) with log;
		end
	commit 
end
go

if not exists(select 1 from sys.procedures where name = 'DeleteConversation' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).DeleteConversation as begin print 1 end')
go
alter procedure $(SchemaName).DeleteConversation
	 @from			sysname
	,@to			sysname
	,@contract		sysname
	,@handle		uniqueidentifier
as begin
	set xact_abort off;
	set nocount on;
	begin tran
	delete from $(SchemaName).Conversations 
	where FromService = @from
		and ToService = @to
		and OnContract = @contract
		and Handle = @handle 
	commit
end
go

if not exists(select 1 from sys.procedures where name = 'GetTopic' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).GetTopic as begin print 1 end')
go
alter procedure $(SchemaName).GetTopic 
	 @from			sysname
	,@to			sysname
	,@contract		sysname
	,@topic			uniqueidentifier
	,@handle		uniqueidentifier output
as begin
	set xact_abort off;
	set nocount on;

	begin transaction 

	select @handle = Handle 
	from $(SchemaName).Topics 
	where FromService = @from
		and ToService = @to
		and OnContract = @contract
		and Topic = @topic

	if @handle is null
	begin
		--there is no available conversation. create one. 
		begin dialog conversation @handle
		from service @from
		to service @to
		on contract @contract
		with encryption = off 

		insert into $(SchemaName).Topics (FromService, ToService, OnContract, Handle, Topic, StartedOn)
		values (@from, @to, @contract, @handle, @topic, sysdatetimeoffset())
	end

	begin conversation timer (@handle) timeout = $(ConversationTimeout) --timeout in seconds. conversation will stay 

	commit 
end
go

if not exists(select 1 from sys.procedures where name = 'DeleteTopic' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).DeleteTopic as begin print 1 end')
go
alter procedure $(SchemaName).DeleteTopic
	 @from			sysname
	,@to			sysname
	,@contract		sysname
	,@handle		uniqueidentifier
as begin
	set xact_abort off;
	set nocount on;
	begin tran
	delete from $(SchemaName).Topics 
	where FromService = @from
		and ToService = @to
		and OnContract = @contract
		and Handle = @handle 
	commit
end
go





if not exists(select 1 from sys.procedures where name = 'SendMessage' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).SendMessage as begin print 1 end')
go
alter procedure $(SchemaName).SendMessage
	 @from			sysname				= null
	,@to			sysname
	,@contract		sysname
	,@messageType	sysname
	,@messageBody	xml					= null
	,@topic			uniqueidentifier	= null
as
begin
	set xact_abort off;
	set nocount on;

	if @messageBody is not null and not @messageBody.exist('/*[1]/@RequestedProcessingOn') = 1
	begin
		--mark when the message was initially sent for processing if it hasn't been marked yet
		declare @RequestedProcessingOn datetimeoffset = sysdatetimeoffset();
		set @messageBody.modify('insert attribute RequestedProcessingOn {sql:variable("@RequestedProcessingOn")} into (/*)[1]');
	end

	declare @handle uniqueidentifier;
	declare @counter int = 0;
	declare @error nvarchar(max)

	--if source service is not provider the local service is assumed
	set @from = isnull(@from, $(SchemaName).GetLocalServiceName());

	while (1=1)
	begin
		--check if there is an available dialog
		if @topic is null
			exec $(SchemaName).GetConversation
				 @from		= @from
				,@to		= @to
				,@contract	= @contract
				,@handle	= @handle output;

		else 
			exec $(SchemaName).GetTopic
				 @from		= @from
				,@to		= @to
				,@contract	= @contract
				,@topic		= @topic
				,@handle	= @handle output;

		begin try 
			if @messageBody is null
				send on conversation @handle message type @messageType
			else 
				send on conversation @handle message type @messageType (@messageBody)

			break;
		end try 
		begin catch 
			set @error = error_message();
		end catch

		--conversation cannot be reused. remove it. try again
		if @topic is null
			exec $(SchemaName).DeleteConversation 
				@from			= @from			
				,@to			= @to			
				,@contract		= @contract		
				,@handle		= @handle;
		else 
			exec $(SchemaName).DeleteTopic
				@from			= @from			
				,@to			= @to			
				,@contract		= @contract		
				,@handle		= @handle;

		select @counter = @counter + 1
		if @counter >= 10
		begin
			raiserror('Failed to send message on conversation. Error: %s', 16, 1, @error) with log;
			break;
		end
	end

	if @topic is null 
		exec $(SchemaName).ReleaseConversation
			 @from			= @from			
			,@to			= @to			
			,@contract		= @contract		
			,@handle		= @handle;
end
go

if not exists(select 1 from sys.procedures where name = 'TriggerAction' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).TriggerAction as begin print 1 end')
go
alter procedure $(SchemaName).TriggerAction
	 @at			sysname			
	,@topic			uniqueidentifier = null		
	,@id			varchar(64) = null
as begin
	set xact_abort off;
	set nocount on;

	declare @messageBody xml = case when @id is not null then (select @id '@Id' for xml path('Process'), type) end
	declare @messageType sysname = case when @id is not null then '$(DomainName)/Messages/TriggerWithId' else '$(DomainName)/Messages/Trigger' end
	exec $(SchemaName).SendMessage
		 @to			= @at
		,@contract		= '$(DomainName)/Contracts/TriggerAction'
		,@messageType	= @messageType
		,@messageBody	= @messageBody
		,@topic			= @topic;
end
go

if not exists(select 1 from sys.procedures where name = 'ProcessData' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ProcessData as begin print 1 end')
go
alter procedure $(SchemaName).ProcessData
	 @at			sysname
	,@data			xml
	,@topic			uniqueidentifier = null
as begin
	set xact_abort off;
	set nocount on;

	exec $(SchemaName).SendMessage
		 @to				= @at
		,@contract			= '$(DomainName)/Contracts/ProcessData'
		,@messageType		= '$(DomainName)/Messages/Data'
		,@messageBody		= @data
		,@topic				= @topic
end
go

if not exists(select 1 from sys.procedures where name = 'ReprocessMessage' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReprocessMessage as begin print 1 end')
go
alter procedure $(SchemaName).ReprocessMessage 
	@id		int 
as
begin
	set xact_abort off;
	set nocount on;
	exec $(SchemaName).TriggerAction 
						 @at = '$(DomainName)/Internal/ReprocessErrors'
						,@id = @id
	print 'Reprocessing has been requested'
end
go

if not exists(select 1 from sys.procedures where name = 'ReactToTimer' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReactToTimer as begin print 1 end')
go
alter procedure $(SchemaName).ReactToTimer
	  @handle		uniqueidentifier 
	 ,@service		sysname
	,@contract		sysname
as begin
	set xact_abort off;
	set nocount on;
	/* special case implmentation
	 
	if @service = 'SpecificService' and @contract = 'SpecificContract'
		send on conversation @handle message type [somespecialtype] ('<xmlmessage />')
	else
		send on conversation @handle message type [$(DomainName)/Messages/EndOfStream]
	 */

	send on conversation @handle message type [$(DomainName)/Messages/EndOfStream]
end
go

if not exists(select 1 from sys.procedures where name = 'ReactToTrigger' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReactToTrigger as begin print 1 end')
go
alter procedure $(SchemaName).ReactToTrigger
	  @service		sysname
	 ,@id			varchar(64) = null
as begin
	set xact_abort off;
	set nocount on;
	/* execute code based on @service */
end
go

if not exists(select 1 from sys.procedures where name = 'ReactToData' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReactToData as begin print 1 end')
go
alter procedure $(SchemaName).ReactToData
	  @service			sysname
	 ,@contract			sysname
	 ,@data				xml
	 ,@attempt			int = null
	 ,@isFinalAttempt	bit = 0
as begin
	set xact_abort off;
	set nocount on;
	/* execute code based on @service */
end
go

if not exists(select 1 from sys.procedures where name = 'ReportError' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ReportError as begin print 1 end')
go
alter procedure $(SchemaName).ReportError 
	 @procedure			sysname
	,@line				int = 0
	,@number			int = 0
	,@message			nvarchar(4000)
	,@context			xml = null
as
begin
	set xact_abort off;
	set nocount on;
	/* implement error reporting */

end
go

if not exists(select 1 from sys.procedures where name = 'ProcessLocalServiceQueue' and schema_id=schema_id('$(SchemaName)'))
exec('create procedure $(SchemaName).ProcessLocalServiceQueue as begin print 1 end')
go
alter procedure $(SchemaName).ProcessLocalServiceQueue
as
begin
	set xact_abort off;
	set nocount on;

	declare @debug					bit = 0;
	declare @handle					uniqueidentifier
		,@service					sysname
		,@contract					sysname
		,@messageId					smallint
		,@messageType				sysname 
		,@messageBody				xml
		,@reprocessing				bit				= 0
		,@id						varchar(64)
		,@counter					int				= 0
		,@attempt					smallint		= 1 
		,@maxAttempts				smallint
		,@messageRetention			bit
		,@recordRetention			bit
		,@ts						datetimeoffset
		,@requestedProcessingOn		datetimeoffset
		,@db						sysname			= db_name()

	while (1=1)
	begin
		set @counter = @counter + 1
		begin tran;
	
		waitfor(
			receive top (1)
				 @handle = [conversation_handle]
				,@service = [service_name]
				,@contract = [service_contract_name]
				,@messageType = [message_type_name]
				,@messageBody = case when validation = 'X' then convert(xml, message_body) end
			from LocalServiceQueue
		), timeout 1000;

		if @@rowcount = 0 or (@counter > 1 and @debug = 1)
		begin
			rollback transaction;
			break;
		end
		
		begin try 
			save transaction localsavepoint 
			set @reprocessing = 0

			/* BEGINNING OF MESSAGE HANDLING */
			/* first messages that cannot be simply retried regardless of conversation status */
			if @messageType = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
					end conversation @handle

			else if @messageType = 'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer'
					exec $(SchemaName).ReactToTimer
							@handle = @handle
						,@service = @service
						,@contract = @contract
			else if @service = '$(DomainName)/Internal/ReprocessErrors' 
				and @messageType = '$(DomainName)/Messages/TriggerWithId'
			begin 
				select @reprocessing = 1
					,@handle = null
					,@id = case when @messageBody is not null then @messageBody.value('(/Process/@Id)[1]', 'varchar(64)') else null end
				select @service = [Service]
					,@contract = [Contract]
					,@messageType = [MessageType]
					,@messageBody = [MessageBody]
					,@attempt = [Attempts]
				from $(SchemaName).ProcessingErrors
				where Id = convert(int, @id)
			end

			if @service is not null
			begin
				set @ts = sysdatetimeoffset()
				set @requestedProcessingOn = case when @messageBody is null then null else @messageBody.value('(/*/@RequestedProcessingOn)[1]', 'datetimeoffset') end

				/* get configuration of the message. in case of reattempt get the configuration for the original message */
				exec $(SchemaName).GetMessageConfiguration
					 @service
					,@contract
					,@messageType
					,@messageId			= @messageId output 
					,@messageRetention	= @messageRetention output 
					,@recordRetention	= @recordRetention output
					,@maxAttempts		= @maxAttempts output 

				save transaction localsavepoint 

				/* process messages including reattempts of errors */
				if @contract = '$(DomainName)/Contracts/TriggerAction' and  @messageType = '$(DomainName)/Messages/Trigger'
					exec $(SchemaName).ReactToTrigger @service = @service
				else if @contract = '$(DomainName)/Contracts/TriggerAction' and  @messageType = '$(DomainName)/Messages/TriggerWithId'
					exec $(SchemaName).ReactToTrigger @service = @service, @id = @id
				else if @messageType = '$(DomainName)/Messages/Data' --@contract = '$(DomainName)/Contracts/ProcessData'
				begin
					declare @isFinalAttempt bit = case when @attempt >= @maxAttempts then 1 else 0 end
					exec $(SchemaName).ReactToData @service = @service, @contract = @contract, @data = @messageBody, @attempt = @attempt
						,@isFinalAttempt = @isFinalAttempt
				end

				/* if it was reprocessing and was successful remove the logged error */
				if @reprocessing=1 
						delete from $(SchemaName).ProcessingErrors where Id = @id 

				insert into $(SchemaName).ProcessingLog (MessageId, Attempt, Successful, RequestedProcessingOn, StartedProcessingOn, FinishedProcessingOn, MessageBody)
				select @messageId, @attempt, 1, @requestedProcessingOn, @ts, sysdatetimeoffset(), case when @messageRetention = 1 then @messageBody else null end
				where @recordRetention = 1

				/* END OF MESSAGE HANDLING */
			end
		end try 
		begin catch 
			declare @errorProcedure sysname			= isnull(error_procedure(),'')
				,@errorLine			int				= isnull(error_line(),-1)
				,@errorNumber		int				= isnull(error_number(),-1)
				,@errorMessage		nvarchar(4000)	= isnull(error_message(),'')

			if xact_state()=-1 
			begin
				--transaction is FUBAR and there is only one option
				rollback transaction 
				--but we still need transcation to log errors and to make sure the commit at the end of the loop has something to do
				begin transaction
			end
			else 
			begin
				rollback transaction localsavepoint 
			end

			--only messages that can be safely retried out of any order should be saved to ProcessingErrors
			--conversations with more significant errors should be terminated with an error message
			declare @attempts table(Id int not null, Attempt int not null)
			if @reprocessing = 0 
			begin
				insert into sb.ProcessingErrors ([Service], [Contract], [MessageType], [MessageBody], [ErrorProcedure], [ErrorLine], [ErrorNumber], [ErrorMessage], Attempts, FirstAttemptOn, LastAttemptOn)
				output inserted.Id, inserted.Attempts into @attempts (Id, Attempt)
				values (@service, @contract, @messageType, @messageBody, @errorProcedure, @errorLine, @errorNumber, @errorMessage, 1, sysdatetimeoffset(), sysdatetimeoffset())
				select @id = (select Id from @attempts)
			end 
			else 
			begin
				update top(1) $(SchemaName).ProcessingErrors
				set Attempts = Attempts+1
					,LastAttemptOn = sysdatetimeoffset()
					,ErrorProcedure = @errorProcedure
					,ErrorLine		= @errorLine
					,ErrorNumber	= @errorNumber
					,ErrorMessage	= @errorMessage
				output inserted.Id, inserted.Attempts into @attempts(Id, Attempt)
				from $(SchemaName).ProcessingErrors
				where Id = convert(int, @id)
			end

			if isnull((select max(attempt) from @attempts), 1) < @maxAttempts
			begin
				--there was an error but we can take another go at processing the message
				raiserror(73001, -1, 1, @db, '$(SchemaName).ProcessLocalServiceQueue', @errorProcedure, @errorLine, @errorNumber, @errorMessage)
				exec $(SchemaName).TriggerAction 
					 @at = '$(DomainName)/Internal/ReprocessErrors'
					,@id = @id
			end
			else 
			begin
				--message failed and this was the final attempt. log permanent error. 
				raiserror(73002, -1, 1, @db, '$(SchemaName).ProcessLocalServiceQueue', @errorProcedure, @errorLine, @errorNumber, @errorMessage)
				begin try 
					save transaction BeforeReportingError
					exec $(schemaName).ReportError @procedure = @errorProcedure, @line = @errorLine, @number = @errorNumber, @message = @errorMessage, @context = @messageBody
				end try 
				begin catch 
					rollback transaction BeforeReportingError
				end catch 
			end

			insert into $(SchemaName).ProcessingLog (MessageId, Attempt, Successful, RequestedProcessingOn, StartedProcessingOn, FinishedProcessingOn, MessageBody)
			select @messageId, @attempt, 0, @requestedProcessingOn, @ts, sysdatetimeoffset(), case when @messageRetention = 1 then @messageBody else null end
			where @recordRetention = 1
		end catch

		commit 
	end 
end
go

/* useful and reusable message types */

if not exists(select 1 from sys.service_message_types where name = '$(DomainName)/Messages/EndOfStream')
create message type [$(DomainName)/Messages/EndOfStream] validation = empty;

if not exists(select 1 from sys.service_message_types where name = '$(DomainName)/Messages/Data')
create message type [$(DomainName)/Messages/Data] validation = well_formed_xml;

if not exists(select 1 from sys.service_message_types where name = '$(DomainName)/Messages/Trigger')
create message type [$(DomainName)/Messages/Trigger] validation = empty;

if not exists(select 1 from sys.service_message_types where name = '$(DomainName)/Messages/TriggerWithId')
create message type [$(DomainName)/Messages/TriggerWithId] validation = well_formed_xml;

if not exists(select 1 from sys.service_contracts where name = '$(DomainName)/Contracts/TriggerAction')
create contract [$(DomainName)/Contracts/TriggerAction] (
	 [$(DomainName)/Messages/Trigger] sent by initiator
	,[$(DomainName)/Messages/TriggerWithId] sent by initiator
	,[$(DomainName)/Messages/EndOfStream] sent by initiator 
);
go

if not exists(select 1 from sys.service_contracts where name = '$(DomainName)/Contracts/ProcessData')
create contract [$(DomainName)/Contracts/ProcessData] (
	 [$(DomainName)/Messages/Data] sent by initiator
	,[$(DomainName)/Messages/EndOfStream] sent by initiator 
);
go

if not exists(select 1 from sys.service_queues where name = 'LocalServiceQueue')
create queue LocalServiceQueue;
go
if not exists(select 1 from sys.services where name = sb.GetLocalServiceName())
begin
	declare @sql nvarchar(max) = 'create service [' + sb.GetLocalServiceName() + '] on queue LocalServiceQueue';
	exec (@sql);
end
go

if not exists(select 1 from sys.services where name = '$(DomainName)/Internal/ReprocessErrors')
begin
	declare @sql nvarchar(max) = 'create service [$(DomainName)/Internal/ReprocessErrors] on queue LocalServiceQueue ([$(DomainName)/Contracts/TriggerAction])';
	exec (@sql);
end
go

alter queue LocalServiceQueue 
	with status = on
	,retention = off
	,activation (
		 status = on
		,procedure_name = $(SchemaName).ProcessLocalServiceQueue
		,max_queue_readers = 3
		,execute as owner
	);
go


/*************TESTING**********************/




--if not exists(select 1 from sys.objects where [type] = 'FN' and name = 'CreateErrorMessage' and schema_id=schema_id('sb'))
--exec('create function sb.CreateErrorMessage() returns xml as begin return(null) end')
--go
--alter function sb.CreateErrorMessage(
--	 @ErrorProcedure	sysname
--	,@ErrorLine			int
--	,@ErrorNumber		int
--	,@ErrorMessage		nvarchar(4000)
--	,@Context			xml
--) returns xml 
--as
--begin
--	return (
--		select 
--			 'Error'				'@Type'
--			,SYSDATETIMEOFFSET()	'@Ts'
--			,@@SERVERNAME			'@SourceServer'
--			,DB_NAME()				'@SourceDb'
--			,@ErrorProcedure		'@Procedure'
--			,@ErrorLine				'@Line'
--			,@ErrorNumber			'@Number'
--			,@ErrorMessage			'Message'
--			,@Context				'*'
--		for xml path('Log')
--	)
--end
--go

--alter procedure sb.ReportError 
--	 @procedure			sysname
--	,@line				int
--	,@number			int
--	,@message			nvarchar(4000)
--	,@context			xml = null
--as
--begin
--	set nocount on;
--	declare @e xml = sb.CreateErrorMessage(@procedure, @line, @number, @message, @context)
--	exec sb.ProcessData
--		 @at			= '//SBL/Logger'
--		,@data			= @e 
--end
--go

