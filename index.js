const { ShardingManager } = require('discord.js');
const Semaphore = require('./semaphore');
const manager = new ShardingManager('./bot.js', {
	token: require("./config.json").TOKEN,
	mode: 'process'
});

const Prefix = "r:"; //just for output


var FreeRenderSlotsUsed = 0;
var PremiumRenderSlotsUsed = 0;
const FreeRenderSlots = 5;
const PremiumRenderSlots = 10;
const CdnDownloadSlots = 20;

var CdnDownloadSemaphore = Semaphore(CdnDownloadSlots);

const CdnDownloadSemaphoreWait = () => CdnDownloadSemaphore.Wait();
const CdnDownloadSemaphoreRelease = () => CdnDownloadSemaphore.Release();

function TakeRenderSlot(premium) {
    let slotType = 0;
    let reply = null;
	if(premium) {
		if(PremiumRenderSlotsUsed >= PremiumRenderSlots) {
			if(FreeRenderSlotsUsed < FreeRenderSlots) {
				FreeRenderSlotsUsed++;
				slotType = 1;
			}
			else {
				reply = "Sorry but I'm already drawing too many, please try again later!";
			}
		}
		else {
			PremiumRenderSlotsUsed++;
			slotType = 2;
		}
	}
	else if(FreeRenderSlotsUsed < FreeRenderSlots) {
		FreeRenderSlotsUsed++;
		slotType = 1;
	}
	else {
		reply = `Sorry but I'm already drawing too many, please try later or consider premium (${Prefix}help premium)`;
	}

	return [slotType, reply];
}
function FreeRenderSlot(slot) {
	if(slot === 1) { FreeRenderSlotsUsed-- } else if(slot === 2) { PremiumRenderSlotsUsed-- }
}

function GetSlots() {
    return [FreeRenderSlotsUsed, PremiumRenderSlotsUsed, CdnDownloadSemaphore.running];
}

const Functions = {
    'TakeRenderSlot': TakeRenderSlot,
    'FreeRenderSlot': FreeRenderSlot,
    'CdnDownloadSemaphoreWait': CdnDownloadSemaphoreWait,
    'CdnDownloadSemaphoreRelease': CdnDownloadSemaphoreRelease,
    'GetSlots': GetSlots
};

function ShardMessage(shard, message) {
    if(!Array.isArray(message)) return;

    let result = Functions[message[0]].apply(null, message[1]);
    if(result !== undefined) {
        if(result instanceof Promise) {
            result.then((result) => shard.eval(`this.ManagerMsg(${JSON.stringify([message[2], result])})`).catch(console.error));
        }
        else {
            shard.eval(`this.ManagerMsg(${JSON.stringify([message[2], result])})`).catch(console.error);
        }
    }
}

manager.on('shardCreate', (shard) => {
    console.log(`Launched shard ${shard.id}`);
    shard.on('message', message => ShardMessage(shard, message));
});
manager.spawn();